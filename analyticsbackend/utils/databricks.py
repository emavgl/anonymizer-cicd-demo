def are_we_on_databricks():
    try:
        from pyspark.dbutils import DBUtils

        # we are running on databricks (no notebook)
        return True
    except ImportError:
        try:
            import IPython

            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if dbutils is None:
                # We are running inside a IPython notebook, no databricks
                return False
            else:
                # We are running on a databricks notebook
                return True
        except (ImportError, AttributeError):
            # Handling AttributeError for those people who have IPython but not on databricks
            # No IPython available, we are running in local, no dbutils available
            return False


def get_dbutils(spark):
    """
    get dbutils instance if you run on databricks
    returns None if no dbutils is available
    """
    dbutils = None
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except ImportError:
        try:
            import IPython

            dbutils = IPython.get_ipython().user_ns.get("dbutils", None)
        except (ImportError, AttributeError):
            pass
    return dbutils


def get_path_for_python_ops(path: str):
    """
    If we are on databricks, paths are refering to dbfs or data-lake.
    For Python operations like 'open' you have to append
    an initial /dbfs/ to indicate the dbfs root mount point.
    """
    path = path.replace("/./", "/")
    if are_we_on_databricks():
        if path.startswith("abfss:"):
            return path  # can't access from python ops?
        if path.startswith("dbfs:"):
            return path.replace("dbfs:/", "/dbfs/")
        if path.startswith("/"):
            return "/dbfs{}".format(path)
        return "/dbfs/{}".format(path)
    return path
