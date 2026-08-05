import logging
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from src.verdanta.common.config import Settings
from src.verdanta.common.paths import landing_dir

logger = logging.getLogger(__name__)



def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_table": f"{load_settings.catalog}.bronze.reference_stores",

        "landing_path": landing_dir(load_settings.landing_root, "reference", "stores"),
        "checkpoint_path": f"{load_settings.checkpoint_base_path}/reference/stores/_checkpoint",
        "schema_location": f"{load_settings.checkpoint_base_path}/reference/stores/_schema",
    }

def _build_stage_bronze_stores(
        source_df: DataFrame,
        run_id: str,
) -> DataFrame:

    df = (
        source_df
        .withColumn("run_id",     lit(run_id))
        .withColumn("_file_name", input_file_name())
        .withColumn("_source_system", lit("reference"))
        .withColumn("_ingest_ts", current_timestamp())
        
    )

    return df

def ingest_bronze_stores(
        spark: SparkSession,
        load_settings: Settings,
) -> None:

    cfg = _build_config(load_settings=load_settings)

    run_id = uuid.uuid4().hex

    logger.info("Bronze stores run start | run_id=%s", run_id)

    try:     

        source_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", cfg["schema_location"])
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .load(cfg["landing_path"])
        )

        bronze_df = _build_stage_bronze_stores(
            source_df=source_df,
            run_id=run_id,
        )

        (
            bronze_df.writeStream
            .option("checkpointLocation", cfg["checkpoint_path"])
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(cfg["bronze_table"])
            .awaitTermination()
        )

    except Exception:
        logger.exception("Bronze stores run FAILED | run_id=%s", run_id)
        raise