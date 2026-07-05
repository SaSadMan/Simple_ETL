from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime



def log_to_db(dag_id, task_id, status, start_time, end_time, error_message=None):
    hook = PostgresHook(postgres_conn_id='postgres_dwh')
    duration = int((end_time - start_time).total_seconds())
    
    hook.run("""
        INSERT INTO etl_logs (
            dag_id, task_id, status, start_time, end_time, duration_seconds, error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, parameters=(
        dag_id, task_id, status, start_time, end_time, duration, error_message
    ))
    
def on_success(context):
    try:
        ti = context['task_instance']
        start_time = ti.start_date.replace(tzinfo=None) if ti.start_date else datetime.now() # сначала пробуем из task_instance
        end_time = datetime.now()
        
        log_to_db(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            status='SUCCESS',
            start_time=start_time,
            end_time=end_time,
            error_message=None
        )
        print("SUCCESS CALLBACK TRIGGERED")
    except Exception as e:
        print(f"ERROR IN SUCCESS CALLBACK: {e}")


def on_failure(context):
    try:
        ti = context['task_instance']
        start_time = ti.start_date.replace(tzinfo=None) if ti.start_date else datetime.now()
        end_time = datetime.now()
        
        log_to_db(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            status='FAILED',
            start_time=start_time,
            end_time=end_time,
            error_message=str(context.get('exception'))
        )
        print("FAILURE CALLBACK TRIGGERED")
    except Exception as e:
        print(f"ERROR IN FAILURE CALLBACK: {e}")

default_args = {
    'on_success_callback': on_success,
    'on_failure_callback': on_failure,
    'start_date': datetime(2026, 3, 23),
}


def get_hwm(dwh, table_name):
    result = dwh.get_first("""
        SELECT last_loaded_at
        FROM dwh_high_water_mark
        WHERE table_name = %s
    """, parameters=(table_name,))

    return result[0] if result else '1900-01-01'


def update_hwm(dwh, table_name, new_hwm):
    dwh.run("""
        INSERT INTO dwh_high_water_mark (table_name, last_loaded_at)
        VALUES (%s, %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at
    """, parameters=(table_name, new_hwm))


def load_table_to_mrr(source_table, target_table, columns, key_columns):
    source = PostgresHook(postgres_conn_id='postgres_operational')
    target = PostgresHook(postgres_conn_id='postgres_mrr')
    dwh = PostgresHook(postgres_conn_id='postgres_dwh')

    hwm_value = get_hwm(dwh, source_table)
    rows = source.get_records(f"""
        SELECT {', '.join(columns)}
        FROM {source_table}
        WHERE updated_at > %s
    """, parameters=(hwm_value,))

    if not rows:
        print(f"No new data for {source_table}")
        return

   
    update_columns = [c for c in columns if c not in key_columns]
    conflict_clause = f"ON CONFLICT ({', '.join(key_columns)}) DO UPDATE SET " + \
                      ", ".join([f"{c} = EXCLUDED.{c}" for c in update_columns])
    
    insert_sql = f"""
        INSERT INTO {target_table} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        {conflict_clause}
    """
    
    for row in rows:
        target.run(insert_sql, parameters=row)

    updated_idx = columns.index('updated_at')
    new_hwm = max(r[updated_idx] for r in rows)
    update_hwm(dwh, source_table, new_hwm)
    print(f"{source_table} loaded, HWM updated to {new_hwm}")


def load_orders_to_mrr():
    load_table_to_mrr(
        'orders',
        'mrr_fact_orders',
        [
            'order_id', 'customer_id', 'employee_id', 'order_date',
            'required_date', 'shipped_date', 'ship_via', 'freight',
            'ship_name', 'ship_address', 'ship_city', 'ship_region',
            'ship_postal_code', 'ship_country', 'updated_at'
        ],
        ['order_id']  
    )


def load_customers_to_mrr():
    load_table_to_mrr(
        'customers',
        'mrr_dim_customers',
        [
            'customer_id', 'company_name', 'contact_name','address',
            'contact_title', 'city', 'region','postal_code', 'country','phone', 'fax', 'updated_at'
        ],
        ['customer_id']
    )


def load_products_to_mrr():
    load_table_to_mrr(
        'products',
        'mrr_dim_products',
        [
            'product_id', 'product_name', 'supplier_id',
            'category_id', 'quantity_per_unit', 'unit_price',
            'units_in_stock','units_on_order', 
            'reorder_level','discontinued','updated_at'
        ],
        ['product_id']
    )


def load_order_details_to_mrr():
    load_table_to_mrr(
        'order_details',
        'mrr_fact_order_details',
        [
            'order_id', 'product_id', 'unit_price',
            'quantity', 'discount', 'updated_at'
        ],
        ['order_id', 'product_id']
    )


def mrr_to_stg():
    mrr = PostgresHook(postgres_conn_id='postgres_mrr')
    stg = PostgresHook(postgres_conn_id='postgres_stg')

    tables = [
        ('mrr_fact_orders', 'stg_fact_orders'),
        ('mrr_dim_customers', 'stg_dim_customers'),
        ('mrr_dim_products', 'stg_dim_products'),
        ('mrr_fact_order_details', 'stg_fact_order_details')
    ]

    for src, tgt in tables:
        stg.run(f"TRUNCATE {tgt}")

        data = mrr.get_records(f"SELECT * FROM {src}")

        if data:
            stg.insert_rows(tgt, data)


def stg_to_dwh():
    stg = PostgresHook(postgres_conn_id='postgres_stg')
    dwh = PostgresHook(postgres_conn_id='postgres_dwh')

    tables = [
        ('stg_fact_orders', 'dwh_fact_orders'),
        ('stg_dim_customers', 'dwh_dim_customers'),
        ('stg_dim_products', 'dwh_dim_products'),
        ('stg_fact_order_details', 'dwh_fact_order_details')
    ]

    for src, tgt in tables:
        dwh.run(f"TRUNCATE {tgt}")

        data = stg.get_records(f"SELECT * FROM {src}")

        if data:
            dwh.insert_rows(tgt, data)

dag = DAG(
    'etl_pipeline2',
    default_args=default_args,
    start_date=datetime(2026, 3, 23),
    schedule='@daily',
    catchup=False
)


t_orders = PythonOperator(
    task_id='load_orders_to_mrr',
    python_callable=load_orders_to_mrr,
    dag=dag
)

t_customers = PythonOperator(
    task_id='load_customers_to_mrr',
    python_callable=load_customers_to_mrr,
    dag=dag
)

t_products = PythonOperator(
    task_id='load_products_to_mrr',
    python_callable=load_products_to_mrr,
    dag=dag
)

t_details = PythonOperator(
    task_id='load_order_details_to_mrr',
    python_callable=load_order_details_to_mrr,
    dag=dag
)

t_stg = PythonOperator(
    task_id='mrr_to_stg',
    python_callable=mrr_to_stg,
    dag=dag
)

t_dwh = PythonOperator(
    task_id='stg_to_dwh',
    python_callable=stg_to_dwh,
    dag=dag
)

[t_orders, t_customers, t_products, t_details] >> t_stg >> t_dwh