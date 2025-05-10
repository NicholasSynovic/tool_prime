from argparse import Namespace

from src.cli import CLI


def main() -> None:
    cli: CLI = CLI()
    ns: Namespace = cli.parse()
    print(ns)


if __name__ == "__main__":
    main()
