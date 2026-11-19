"""Point d'entrée : exécute une étape ou tout le pipeline.

    spark-submit src/main.py --step all
    spark-submit src/main.py --step silver
"""
import argparse
import logging
import time

from .bronze import ingest
from .config import load_config
from .generate import generate
from .gold import build_gold
from .marts import build_marts
from .silver import build_silver
from .spark import get_spark

ETAPES = {
    "generate": generate,
    "bronze": ingest,
    "silver": build_silver,
    "gold": build_gold,
    "marts": build_marts,
}

log = logging.getLogger("telecom_pipeline")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline Big Data télécom (PySpark)")
    parser.add_argument("--step", choices=[*ETAPES, "all"], default="all")
    parser.add_argument("--config", default="config/pipeline.yaml")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    cfg = load_config(args.config)
    spark = get_spark(cfg["spark"])
    spark.sparkContext.setLogLevel("WARN")

    etapes = list(ETAPES) if args.step == "all" else [args.step]
    debut_total = time.perf_counter()
    try:
        for etape in etapes:
            debut = time.perf_counter()
            log.info("▶ %s", etape)
            ETAPES[etape](spark, cfg)
            log.info("✔ %s terminé en %.1f s", etape, time.perf_counter() - debut)
        log.info("Pipeline terminé en %.1f s", time.perf_counter() - debut_total)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
