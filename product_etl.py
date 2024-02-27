import pypyodbc
import random
import product_config
import product_query
import etl_config


def cleanup(connection, given_cursor):
    given_cursor.execute(product_query.DELETE_PRODUCT_COST_QUERY)
    given_cursor.execute(product_query.DELETE_PRODUCT_DEMAND_QUERY)
    given_cursor.execute(product_query.DELETE_MANUFACTURING_CAPACITY_QUERY)
    given_cursor.execute(product_query.DELETE_PRODUCT_QUERY)
    connection.commit()


def create_products(connection, given_cursor):
    for product_name in product_config.PRODUCTS:
        given_cursor.execute(product_query.INSERT_PRODUCT_QUERY.format(product_name_var=product_name))
    connection.commit()


def create_product_cost(connection, given_cursor, given_product_id):
    for lot in product_config.AVAILABLE_LOTS:
        fixed_cost = round(lot * random.uniform(product_config.FIXED_UNIT_COST_RANGE_START,
                                                product_config.FIXED_UNIT_COST_RANGE_END), 2)
        variable_cost = round(random.uniform(product_config.UNIT_COST_RANGE_START,
                                             product_config.UNIT_COST_RANGE_END), 2)
        out_sourcing_cost = round(((fixed_cost / lot) + variable_cost)
                                  * (1 + random.uniform(product_config.OUTSOURCING_COST_FACTOR_START,
                                                        product_config.OUTSOURCING_COST_FACTOR_END)), 2)
        cost_sql = (product_query.INSERT_PRODUCT_COST_QUERY
                    .format(product_id_var=given_product_id,
                            lot_var=lot,
                            fixed_cost_var=fixed_cost,
                            variable_cost_var=variable_cost,
                            out_sourcing_cost_var=out_sourcing_cost))
        given_cursor.execute(cost_sql)
    connection.commit()


def create_manufacturing_capacity(connection, given_cursor, given_product_id, given_month):
    manufacturing_uncertainty = round(random.uniform(product_config.MANUFACTURING_UNCERTAINTY_RANGE_START,
                                                     product_config.MANUFACTURING_UNCERTAINTY_RANGE_END), 4)
    manufacturing_capacity = int(round(max(product_config.AVAILABLE_LOTS)
                                       * (1 + manufacturing_uncertainty), 0))
    manu_sql = product_query.INSERT_MANUFACTURING_CAPACITY_QUERY.format(
        given_product_id_var=given_product_id,
        given_month_var=given_month,
        manufacturing_capacity_var=manufacturing_capacity)
    given_cursor.execute(manu_sql)
    connection.commit()


def create_demand_and_sales(connection, given_cursor, given_product_id,
                            given_month, given_base_demand,
                            given_slope_factor, given_sales_file):
    sales_factor = round(random.uniform(product_config.SALES_FACTOR_RANGE_START,
                                        product_config.SALES_FACTOR_RANGE_END), 4)

    for markup in product_config.PRICE_MARKUPS:
        demand = round(given_base_demand * (1 - markup) * (1 + given_slope_factor), 2)
        actual_sale = round(demand * (1 + sales_factor), 2)
        for region in product_config.AVAILABLE_REGIONS:
            regional_demand = int(round(demand *
                                        (1 + product_config
                                         .REGIONAL_BIAS[product_config.AVAILABLE_REGIONS.index(region)]), 0))
            demand_sql = product_query.INSERT_PRODUCT_DEMAND_QUERY.format(
                given_product_id_var=given_product_id,
                region_var=region, given_month_var=given_month,
                markup_var=markup, regional_demand_var=regional_demand)
            given_cursor.execute(demand_sql)
            connection.commit()
            regional_sale = int(round(actual_sale * (1 + product_config.REGIONAL_BIAS[
                product_config.AVAILABLE_REGIONS.index(region)]), 0))
            given_sales_file.write(f"{given_product_id},{region},{given_month},{markup},{regional_sale}\n")


def main():
    conn = pypyodbc.connect(etl_config.CONNECTION_STR)
    cursor = conn.cursor()

    cleanup(conn, cursor)
    create_products(conn, cursor)

    slope_factor = 0.0

    sales_file = open(etl_config.CSV_FILE, 'w')
    sales_file.write(etl_config.CSV_FILE_HEADER)
    cursor.execute(product_query.SELECT_PRODUCT_QUERY)
    rows = cursor.fetchall()

    for row in rows:
        product_id = row['product_id']
        create_product_cost(conn, cursor, product_id)
        base_demand = round(random.uniform(min(product_config.AVAILABLE_LOTS),
                                           max(product_config.AVAILABLE_LOTS)), 0)
        for month in range(1, 12):
            create_manufacturing_capacity(conn, cursor, product_id, month)
            create_demand_and_sales(conn, cursor, product_id, month,
                                    base_demand, slope_factor, sales_file)
            slope_factor = slope_factor + product_config.SLOPE_INCREMENT
    sales_file.close()
    conn.close()


if __name__ == "__main__":
    main()
