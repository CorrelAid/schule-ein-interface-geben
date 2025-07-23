import duckdb
import polars as pl
import polars.selectors as cs

def transform_api_results(pipeline_name, db_name):
    with duckdb.connect(f"{pipeline_name}.duckdb", read_only=False) as db:
        df_posts = db.sql(f"SELECT * FROM {db_name}.posts").pl().rename({"title__rendered": "title", "content__rendered": "content" })
        df_stufe = db.sql(f"SELECT * FROM {db_name}.stufe").pl()
        df_posts_stufe = db.sql(f"SELECT * FROM {db_name}.posts__stufe").pl()

        df_categories = db.sql(f"SELECT * FROM {db_name}.categories").pl()
        df_posts_categories = db.sql(f"SELECT * FROM {db_name}.posts__categories").pl()

        df_tags = db.sql(f"SELECT * FROM {db_name}.tags").pl()
        df_posts_tags = db.sql(f"SELECT * FROM {db_name}.posts__tags").pl()

    df = df_posts.join(df_posts_stufe, left_on="_dlt_id", right_on="_dlt_parent_id",how="full").rename({"value": "stufe_id"}).drop("_dlt_id_right")

    df = df.join(df_stufe, left_on="stufe_id", right_on="id",how="full").rename({"slug_right": "stage"}).drop(cs.ends_with("right"))

    df_ts = df.join(df_posts_categories, left_on="_dlt_id", right_on="_dlt_parent_id", how="full").rename({"value": "tool_type_id"}).drop(cs.ends_with("right"))\
        .join(df_categories, left_on="tool_type_id", right_on="id", how="full").rename({"slug_right": "tool_types"}).group_by("id").agg(pl.col("tool_types").explode())
    df = df.join(df_ts, left_on="id", right_on="id")

    df_tts = df.join(df_posts_tags, left_on="_dlt_id", right_on="_dlt_parent_id", how="full").rename({"value": "topic_id"}).drop(cs.ends_with("right"))\
        .join(df_tags, left_on="topic_id", right_on="id", how="full").rename({"slug_right": "topics"}).group_by("id").agg(pl.col("topics").drop_nulls().explode())
    df = df.join(df_tts, left_on="id", right_on="id")

    df = df.with_columns(
    pl.col("date").dt.truncate("1d"))

    return df[["id","date","title","content", "stage", "topics", "tool_types"]]
