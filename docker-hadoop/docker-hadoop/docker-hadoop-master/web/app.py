# web/app.py
import sys
import subprocess
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template, jsonify

app = Flask(__name__)

# web 目录与项目根目录
BASE_DIR = Path(__file__).resolve().parent          # .../web
PROJECT_DIR = BASE_DIR.parent                       # .../docker-hadoop-master

# 文件路径
TOP20_FILE = PROJECT_DIR / "top20.txt"
SEG_FILE = PROJECT_DIR / "news_seg.txt"

# Hadoop & HDFS（容器内绝对路径）
HADOOP_BIN = "/opt/hadoop-3.2.1/bin/hadoop"
HDFS_BIN = "/opt/hadoop-3.2.1/bin/hdfs"
HADOOP_STREAMING_JAR = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

CONTAINER = "namenode"


def run_cmd(args: list[str]) -> tuple[int, str]:
    """以 list 方式执行命令，避免 Windows 引号/转义问题。"""
    print("执行命令:", " ".join(args))
    try:
        p = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            check=False,
        )
        out = p.stdout or ""
        return p.returncode, out
    except Exception as e:
        return 1, f"[run_cmd] 执行异常: {e}"


def ensure_container_tmp() -> tuple[int, str]:
    """确保容器临时目录可用（修复 tmpDir/null、/tmp 权限等）"""
    cmd = [
        "docker", "exec", CONTAINER, "bash", "-lc",
        "mkdir -p /hadoop_tmp && chmod 1777 /hadoop_tmp && "
        "chmod 1777 /tmp || true && "
        "ls -ld /tmp /hadoop_tmp"
    ]
    return run_cmd(cmd)


def parse_top20(path: Path):
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    data = []
    for line in lines:
        if "\t" not in line:
            continue
        w, c = line.split("\t", 1)
        try:
            c = int(c)
        except Exception:
            continue
        data.append({"word": w, "count": c})
    data.sort(key=lambda x: x["count"], reverse=True)
    return data[:20]


@app.get("/")
def index():
    top20 = parse_top20(TOP20_FILE)
    return render_template("index.html", top20=top20)


@app.post("/api/refresh")
def api_refresh():
    LOG_FILE = PROJECT_DIR / "last_refresh.log"

    def dump_logs():
        LOG_FILE.write_text("\n".join(logs), encoding="utf-8", errors="ignore")

    """
    全流程：抓取 -> 分词 -> 复制到容器 -> HDFS put -> Hadoop streaming -> getmerge -> 拷回结果
    """
    logs: list[str] = []
    try:
        logs.append(f"PROJECT_DIR={PROJECT_DIR}")

        # 0) 容器 tmp 兜底
        rc, out = ensure_container_tmp()
        logs.append(out)
        if rc != 0:
            dump_logs()
            return jsonify({"ok": False, "msg": "容器临时目录初始化失败", "detail": "\n".join(logs)}), 500

        # 1) 抓取
        rc, out = run_cmd([sys.executable, str(PROJECT_DIR / "fetch_hot.py")])
        logs.append(out)
        if rc != 0:
            dump_logs()
            return jsonify({"ok": False, "msg": "抓取失败", "detail": "\n".join(logs)}), 500

        # 2) 分词
        rc, out = run_cmd([sys.executable, str(PROJECT_DIR / "segment.py")])
        logs.append(out)
        if rc != 0:
            dump_logs()
            return jsonify({"ok": False, "msg": "分词失败", "detail": "\n".join(logs)}), 500

        # 3) 检查容器 python3，不行就用 python
        py = "python3"
        rc, out = run_cmd(["docker", "exec", CONTAINER, "python3", "-V"])
        logs.append(out)
        if rc != 0:
            dump_logs()
            py = "python"
            rc2, out2 = run_cmd(["docker", "exec", CONTAINER, "python", "-V"])
            logs.append(out2)
            if rc2 != 0:
                dump_logs()
                return jsonify({"ok": False, "msg": "容器内找不到 python/python3", "detail": "\n".join(logs)}), 500

        # 4) 拷贝 mapper/reducer/seg 到容器根目录
        for fname in ["mapper.py", "reducer.py", "news_seg.txt"]:
            src = PROJECT_DIR / fname
            if not src.exists():
                return jsonify({"ok": False, "msg": f"缺少文件：{src}", "detail": "\n".join(logs)}), 500

            rc, out = run_cmd(["docker", "cp", str(src), f"{CONTAINER}:/{fname}"])
            logs.append(out)
            if rc != 0:
                dump_logs()
                return jsonify({"ok": False, "msg": f"docker cp 失败：{fname}", "detail": "\n".join(logs)}), 500

        # 5) HDFS：清理输出/准备输入/上传
        run_cmd(["docker", "exec", CONTAINER, HDFS_BIN, "dfs", "-rm", "-r", "-f", "/output/result"])
        run_cmd(["docker", "exec", CONTAINER, HDFS_BIN, "dfs", "-mkdir", "-p", "/input"])

        rc, out = run_cmd(["docker", "exec", CONTAINER, HDFS_BIN, "dfs", "-put", "-f", "/news_seg.txt", "/input/"])
        logs.append(out)
        if rc != 0:
            return jsonify({"ok": False, "msg": "上传 HDFS 失败", "detail": "\n".join(logs)}), 500

        # 6) Hadoop Streaming（关键：-D 指定 tmp）
        streaming_args = [
            "docker", "exec", CONTAINER,
            HADOOP_BIN, "jar", HADOOP_STREAMING_JAR,
            "-Dmapreduce.framework.name=local",
            "-Dmapreduce.job.reduces=1",
            "-Djava.io.tmpdir=/hadoop_tmp",
            "-Dhadoop.tmp.dir=/hadoop_tmp",
            "-Dmapreduce.job.local.dir=/hadoop_tmp",
            "-Dmapreduce.cluster.local.dir=/hadoop_tmp",
            "-files", "/mapper.py,/reducer.py",
            "-mapper", f"{py} mapper.py",
            "-reducer", f"{py} reducer.py",
            "-input", "/input/news_seg.txt",
            "-output", "/output/result",
        ]
        rc, out = run_cmd(streaming_args)
        logs.append(out)
        if rc != 0:
            return jsonify({"ok": False, "msg": "Hadoop计算失败", "detail": "\n".join(logs)}), 500

        # 7) 合并输出并拷回
        run_cmd(["docker", "exec", CONTAINER, "rm", "-f", "/top20.txt"])

        rc, out = run_cmd(["docker", "exec", CONTAINER, HDFS_BIN, "dfs", "-getmerge", "/output/result", "/top20.txt"])
        logs.append(out)
        if rc != 0:
            return jsonify({"ok": False, "msg": "getmerge 失败", "detail": "\n".join(logs)}), 500

        rc, out = run_cmd(["docker", "cp", f"{CONTAINER}:/top20.txt", str(TOP20_FILE)])
        logs.append(out)
        if rc != 0:
            return jsonify({"ok": False, "msg": "拷回 top20.txt 失败", "detail": "\n".join(logs)}), 500

        # 8) 返回
        top20 = parse_top20(TOP20_FILE)
        return jsonify({
            "ok": True,
            "msg": "刷新成功",
            "top20": top20,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "detail": "\n".join(logs),  # 你调试用：前端不想显示就不显示
        })

    except Exception as e:
        logs.append(f"[api_refresh] 异常: {e}")
        return jsonify({"ok": False, "msg": "后端异常", "detail": "\n".join(logs)}), 500


if __name__ == "__main__":
    print("--- 路径调试信息 ---")
    print("BASE_DIR:", BASE_DIR)
    print("PROJECT_DIR:", PROJECT_DIR)
    app.run(host="0.0.0.0", port=5000, debug=True)
