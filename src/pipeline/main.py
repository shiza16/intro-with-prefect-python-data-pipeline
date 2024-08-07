#!/usr/bin/env python3
import httpx
import psycopg2
from prefect import flow, get_run_logger, task

@task
def retrieve_from_api(
    base_url :str,
    path : str,
    secure : bool
):
    logger = get_run_logger()
    if secure:
        url = f"https://{base_url}{path}"
    else:
        url = f"https://{base_url}{path}"
    response = httpx.get(url)
    response.raise_for_status()
    inventory_stats = response.json()
    logger.info(inventory_stats)
    return inventory_stats

@task
def clean_data_stats(inventory_stats: dict) ->  dict:
    return{
        "sold": inventory_stats.get("sold",0) + inventory_stats.get("Sold",0) ,
        "not available" : inventory_stats.get("not available",0) + inventory_stats.get("Not Available",0),
        "available" : inventory_stats.get("available",0) + inventory_stats.get("Available",0),
        "pending" : inventory_stats.get("pending",0) + inventory_stats.get("Pending",0)

    }

@task
def insert_to_db(
    inventory_stats: dict,
    db_user: str,
    db_password: str,
    db_name: str,
    db_host: str,
):
    logger = get_run_logger()
    try:
        with psycopg2.connect(
            user=db_user, password=db_password, dbname=db_name, host=db_host
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into inventory_history (
                                            fetch_timestamp,
                                            sold,
                                            pending,
                                            available,
                                            unavailable
                    ) 
                    values (now(), %(sold)s, %(pending)s, %(available)s, %(unavailable)s)""",
                    inventory_stats,
                )
                logger.info("Done")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")


@flow
def collect_petstore_inventory(
    base_url: str = "petstore.swagger.io",
    path: str = "/v2/store/inventory",
    secure: bool = True,
    db_user: str = "root",
    db_password: str = "root",
    db_name: str = "petstore",
    db_host: str = "localhost"
):
    inventory_stats = retrieve_from_api(base_url,path,secure)
    inventory_stats = clean_data_stats(inventory_stats)
    insert_to_db(
        inventory_stats,
        db_user,
        db_password,
        db_name,
        db_host,
    )
    


def main():
    collect_petstore_inventory.serve("petstore-collection-deployment")


if __name__ == "__main__":
    main()
