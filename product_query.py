DELETE_PRODUCT_COST_QUERY = "DELETE FROM product_cost"
DELETE_PRODUCT_DEMAND_QUERY = "DELETE FROM product_demand"
DELETE_MANUFACTURING_CAPACITY_QUERY = "DELETE FROM manufacturing_capacity"
DELETE_PRODUCT_QUERY = "DELETE FROM product"

INSERT_PRODUCT_QUERY = "INSERT INTO product (product_name) VALUES ('{product_name_var}')"
INSERT_PRODUCT_COST_QUERY = '''INSERT INTO product_cost 
                            (product_id,fixed_cost_qty,fixed_cost,unit_cost,outsourcing_unit_cost) 
                            VALUES ({product_id_var},{lot_var},{fixed_cost_var},{variable_cost_var},
                            {out_sourcing_cost_var})'''
INSERT_MANUFACTURING_CAPACITY_QUERY = '''INSERT INTO manufacturing_capacity 
                                        (product_id,month,qty) 
                                        VALUES ({given_product_id_var},{given_month_var},
                                        {manufacturing_capacity_var})'''
INSERT_PRODUCT_DEMAND_QUERY = '''INSERT INTO product_demand 
                                (product_id,region_code,month,cost_markup_percent,demand_qty) 
                                VALUES ({given_product_id_var},'{region_var}',{given_month_var},
                                {markup_var},{regional_demand_var})'''

SELECT_PRODUCT_QUERY = "SELECT * FROM product"
