from prefect import flow
import duckdb

@flow(log_prints=True)
def my_flow():
    con = duckdb.connect("md:?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImFraXJhQHNhbmdvLXRlY2guY29tIiwic2Vzc2lvbiI6ImFraXJhLnNhbmdvLXRlY2guY29tIiwicGF0IjoiN28wZmdtTEo3REFqV0VnWjV4TVVwUGhUREdhMVRZaGRiMUpYeExGYW9vSSIsInVzZXJJZCI6Ijc2ZmQ2YTFlLWE2NGEtNDhhNi1iMGE0LWI4MjFlODRmZWQyOCIsImlzcyI6Im1kX3BhdCIsImlhdCI6MTcyOTgyOTk1NH0.Z1FO2MLAiYD0uNg5aVOu75NyIDy5ZbfM8Tw9yGBxxRw")
    df = con.sql("SELECT COUNT(*) FROM orders2")
    print(df)


if __name__ == "__main__":
    my_flow.serve(
        name="run-on-local",
        interval=60
    )
