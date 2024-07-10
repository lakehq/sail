from framework import _native

SparkConnectServer = _native.spark.server.SparkConnectServer

__all__ = [
    "SparkConnectServer",
]


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    server = SparkConnectServer(args.ip, args.port)
    server.start()


if __name__ == "__main__":
    main()
