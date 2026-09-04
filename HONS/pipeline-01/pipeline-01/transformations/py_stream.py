import dlt


@dlt.table()
def food_sales():
    return (
        spark.readStream.format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option('pathGlobFilter', '*.csv')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .load('/Volumes/kishore_d/default/csv/')
    )


@dlt.materialized_view()
def food_sales_v():
    return spark.sql('select MenuItem,sum(Quantity) as total_quantity_01 from food_sales group by MenuItem order by total_quantity_01 desc')


@dlt.materialized_view()
def food_sales_v1():
    return spark.sql('select OrderID, avg(Quantity) as avg_food_orders from food_sales group by OrderID order by OrderID Desc')


@dlt.materialized_view()
def food_sales_view():
    return spark.sql('select count(*) as total_count from food_sales')