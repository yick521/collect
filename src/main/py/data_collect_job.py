import json
import subprocess
import os
import sys
from datetime import datetime

def main():
    try:
        # 1. 读取标准输入中的完整JSON参数
        config_data = ""

        # 优先从命令行参数读取文件路径
        if len(sys.argv) > 1:
            config_file_path = sys.argv[1]
            if os.path.exists(config_file_path):
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    config_data = f.read()
                print(f"从文件读取配置: {config_file_path}")

                # ✅ 清理外层引号和转义字符
                config_data = config_data.strip()

                # 移除外层的单引号包装: '...' -> ...
                if config_data.startswith("'") and config_data.endswith("'"):
                    config_data = config_data[1:-1]
                    print("已移除外层单引号")

                # 移除外层的双引号包装: "..." -> ...
                if config_data.startswith('"') and config_data.endswith('"'):
                    config_data = config_data[1:-1]
                    print("已移除外层双引号")

                # 反转义内部的引号
                # \' -> '
                # \" -> "
                # \\ -> \
                # config_data = config_data.replace("\\'", "'")
                # config_data = config_data.replace('\\"', '"')
                # config_data = config_data.replace("\\\\", "\\")

                print(f"清理后JSON长度: {len(config_data)}")

            else:
                config_data = sys.argv[1]  # 直接作为JSON字符串
                print("从命令行参数读取配置")

        # 验证 JSON 格式是否正确（但不使用解析后的对象）
        try:
            print(f"接收到的原始参数: {repr(config_data)}")
            json.loads(config_data)
            print("JSON格式验证通过")
        except json.JSONDecodeError as e:
            print(f"JSON格式错误: {e}")
            print(f"错误位置: 第{e.pos}个字符")
            print(f"错误附近内容: {repr(config_data[max(0, e.pos-50):min(len(config_data), e.pos+50)])}")
            sys.exit(1)

        # 2. 生成任务配置文件到当前目录
        task_config_filename = "task.json"

        # 3. 直接写入原始 JSON 字符串（不进行解析和重新序列化）
        with open(task_config_filename, 'w', encoding='utf-8') as config_file:
            config_file.write(config_data)

        print(f"已生成任务配置文件: {task_config_filename}")

        # 4. 验证 JAR 文件存在
        jar_path = "./data-collect-job.jar"
        if not os.path.exists(jar_path):
            print(f"错误: JAR文件不存在: {jar_path}")
            sys.exit(1)

        # 5. 构建Java命令（只构建一次，避免重复）
        app_config_path = "./data-collect-job-application.yml"

        print("检测到Spring Boot Fat JAR，使用java -jar方式运行")
        java_command = ["java", "-jar", jar_path]

        # 添加Spring配置 - 只添加一次
        if os.path.exists(app_config_path):
            java_command.extend([
                f"--spring.config.location=file:./{app_config_path}",
                "--spring.profiles.active=dev",
                "--spring.main.web-application-type=none",  # 禁用web服务器
                "--datax.task.config-file=file:./task.json"  # 指定任务配置文件
            ])
            print(f"使用应用配置文件: {app_config_path}")
            print("激活profile: dev")
            print("禁用web服务器")
            print("指定任务配置文件: task.json")

        print(f"执行命令: {' '.join(java_command)}")
        print("-" * 50)

        # 6. 使用兼容的subprocess.run方式
        print("方式1: 直接执行并实时显示输出")
        try:
            result = subprocess.run(
                java_command,
                universal_newlines=True,  # 兼容Python 3.6-
                bufsize=1,
                # 不捕获输出，直接显示到控制台
                stdout=None,
                stderr=None
            )

            print("-" * 50)
            if result.returncode != 0:
                print(f"任务执行失败，返回码: {result.returncode}")
                sys.exit(1)
            else:
                print("任务执行成功！")

        except KeyboardInterrupt:
            print("\n收到中断信号，程序已终止")
            sys.exit(1)

    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {str(e)}", file=sys.stderr)
        print("请检查输入的JSON格式是否正确", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"文件未找到错误: {str(e)}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"未知错误: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()