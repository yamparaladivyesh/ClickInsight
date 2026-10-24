from pyspark.sql import DataFrame
from pyspark.sql import functions as F
FUNNEL_STAGES = [('Product View', 'page_view', 1), ('Add to Cart', 'add_to_cart', 2), ('Checkout', 'checkout', 3), ('Purchase', 'purchase', 4)]

def _first_event(events: DataFrame, event_type: str) -> DataFrame:
    """
    Find the first occurrence of event_type in every session.

    Ordering is based on event_sequence.

    Returns:
        session_id
        sequence
        event_time
    """
    filtered = events.filter(F.col('event_type') == event_type).select('session_id', 'event_sequence', 'timestamp')
    first_sequence = filtered.groupBy('session_id').agg(F.min('event_sequence').alias('sequence'))
    result = first_sequence.alias('first').join(filtered.alias('events'), (F.col('first.session_id') == F.col('events.session_id')) & (F.col('first.sequence') == F.col('events.event_sequence')), 'inner').select(F.col('first.session_id').alias('session_id'), F.col('first.sequence').alias('sequence'), F.col('events.timestamp').alias('event_time'))
    return result

def _first_event_after(events: DataFrame, event_type: str, previous_stage: DataFrame) -> DataFrame:
    """
    Find the first event_type after the previous valid stage.

    Ordering is based on event_sequence.
    """
    filtered = events.filter(F.col('event_type') == event_type).select('session_id', 'event_sequence', 'timestamp')
    eligible = previous_stage.alias('previous').join(filtered.alias('events'), F.col('previous.session_id') == F.col('events.session_id'), 'inner').filter(F.col('events.event_sequence') > F.col('previous.sequence')).select(F.col('events.session_id').alias('session_id'), F.col('events.event_sequence').alias('sequence'), F.col('events.timestamp').alias('event_time'))
    first_sequence = eligible.groupBy('session_id').agg(F.min('sequence').alias('sequence'))
    result = first_sequence.alias('first').join(eligible.alias('eligible'), (F.col('first.session_id') == F.col('eligible.session_id')) & (F.col('first.sequence') == F.col('eligible.sequence')), 'inner').select(F.col('first.session_id').alias('session_id'), F.col('first.sequence').alias('sequence'), F.col('eligible.event_time').alias('event_time'))
    return result

def build_funnel_sessions(sessionized_events: DataFrame) -> DataFrame:
    """
    Build one strict funnel record per session.

    Strict progression:

        Product View
            ↓
        Add to Cart
            ↓
        Checkout
            ↓
        Purchase

    A stage is valid only if it occurs after the
    previous valid stage.
    """
    events = sessionized_events.select('session_id', 'event_sequence', 'timestamp', 'event_type')
    product_view = _first_event(events, 'page_view').select('session_id', F.col('sequence').alias('product_view_sequence'), F.col('event_time').alias('product_view_time'))
    add_to_cart = _first_event_after(events, 'add_to_cart', product_view.select('session_id', F.col('product_view_sequence').alias('sequence'))).select('session_id', F.col('sequence').alias('add_to_cart_sequence'), F.col('event_time').alias('add_to_cart_time'))
    checkout = _first_event_after(events, 'checkout', add_to_cart.select('session_id', F.col('add_to_cart_sequence').alias('sequence'))).select('session_id', F.col('sequence').alias('checkout_sequence'), F.col('event_time').alias('checkout_time'))
    purchase = _first_event_after(events, 'purchase', checkout.select('session_id', F.col('checkout_sequence').alias('sequence'))).select('session_id', F.col('sequence').alias('purchase_sequence'), F.col('event_time').alias('purchase_time'))
    funnel_sessions = product_view.join(add_to_cart, 'session_id', 'left').join(checkout, 'session_id', 'left').join(purchase, 'session_id', 'left')
    funnel_sessions = funnel_sessions.withColumn('furthest_stage', F.when(F.col('purchase_sequence').isNotNull(), 'Purchase').when(F.col('checkout_sequence').isNotNull(), 'Checkout').when(F.col('add_to_cart_sequence').isNotNull(), 'Add to Cart').otherwise('Product View')).withColumn('furthest_stage_order', F.when(F.col('purchase_sequence').isNotNull(), 4).when(F.col('checkout_sequence').isNotNull(), 3).when(F.col('add_to_cart_sequence').isNotNull(), 2).otherwise(1))
    return funnel_sessions

def calculate_funnel_summary(funnel_sessions: DataFrame) -> DataFrame:
    """
    Calculate:

        - stage sessions
        - previous stage sessions
        - conversion rate
        - drop-off rate
    """
    counts = funnel_sessions.agg(F.count('*').alias('product_view_sessions'), F.sum(F.when(F.col('add_to_cart_sequence').isNotNull(), 1).otherwise(0)).alias('add_to_cart_sessions'), F.sum(F.when(F.col('checkout_sequence').isNotNull(), 1).otherwise(0)).alias('checkout_sessions'), F.sum(F.when(F.col('purchase_sequence').isNotNull(), 1).otherwise(0)).alias('purchase_sessions')).first()
    product_view = int(counts['product_view_sessions'])
    add_to_cart = int(counts['add_to_cart_sessions'])
    checkout = int(counts['checkout_sessions'])
    purchase = int(counts['purchase_sessions'])
    rows = [('Product View', 1, product_view, None, None, None), ('Add to Cart', 2, add_to_cart, product_view, add_to_cart / product_view if product_view > 0 else 0.0, 1.0 - add_to_cart / product_view if product_view > 0 else 0.0), ('Checkout', 3, checkout, add_to_cart, checkout / add_to_cart if add_to_cart > 0 else 0.0, 1.0 - checkout / add_to_cart if add_to_cart > 0 else 0.0), ('Purchase', 4, purchase, checkout, purchase / checkout if checkout > 0 else 0.0, 1.0 - purchase / checkout if checkout > 0 else 0.0)]
    spark = funnel_sessions.sparkSession
    return spark.createDataFrame(rows, ['stage', 'stage_order', 'sessions', 'previous_stage_sessions', 'conversion_rate', 'dropoff_rate'])

def calculate_funnel_time_metrics(funnel_sessions: DataFrame) -> DataFrame:
    """
    Calculate time between observed funnel events.

    Metrics:

        Product View → Add to Cart
        Add to Cart → Checkout
        Checkout → Purchase
        Product View → Purchase

    Statistics:

        count
        mean
        median
        P25
        P75
        P90
        standard deviation

    Units:
        minutes
    """
    durations = funnel_sessions.select('session_id', ((F.col('add_to_cart_time').cast('double') - F.col('product_view_time').cast('double')) / 60.0).alias('product_view_to_cart'), ((F.col('checkout_time').cast('double') - F.col('add_to_cart_time').cast('double')) / 60.0).alias('cart_to_checkout'), ((F.col('purchase_time').cast('double') - F.col('checkout_time').cast('double')) / 60.0).alias('checkout_to_purchase'), ((F.col('purchase_time').cast('double') - F.col('product_view_time').cast('double')) / 60.0).alias('product_view_to_purchase'))
    metric_definitions = [('Product View → Add to Cart', 'product_view_to_cart'), ('Add to Cart → Checkout', 'cart_to_checkout'), ('Checkout → Purchase', 'checkout_to_purchase'), ('Product View → Purchase', 'product_view_to_purchase')]
    rows = []
    for metric_name, column_name in metric_definitions:
        stats = durations.filter(F.col(column_name).isNotNull()).agg(F.count(column_name).alias('count'), F.avg(column_name).alias('mean'), F.expr(f'percentile_approx(`{column_name}`, 0.50)').alias('median'), F.expr(f'percentile_approx(`{column_name}`, 0.25)').alias('p25'), F.expr(f'percentile_approx(`{column_name}`, 0.75)').alias('p75'), F.expr(f'percentile_approx(`{column_name}`, 0.90)').alias('p90'), F.stddev(column_name).alias('stddev')).first()
        rows.append((metric_name, int(stats['count']), float(stats['mean']) if stats['mean'] is not None else None, float(stats['median']) if stats['median'] is not None else None, float(stats['p25']) if stats['p25'] is not None else None, float(stats['p75']) if stats['p75'] is not None else None, float(stats['p90']) if stats['p90'] is not None else None, float(stats['stddev']) if stats['stddev'] is not None else None))
    spark = funnel_sessions.sparkSession
    return spark.createDataFrame(rows, ['metric', 'count', 'mean_minutes', 'median_minutes', 'p25_minutes', 'p75_minutes', 'p90_minutes', 'stddev_minutes'])

def calculate_funnel_trends(funnel_sessions: DataFrame) -> DataFrame:
    """
    Calculate daily funnel trends.
    """
    daily = funnel_sessions.withColumn('date', F.to_date('product_view_time')).groupBy('date').agg(F.count('*').alias('sessions'), F.sum(F.when(F.col('add_to_cart_sequence').isNotNull(), 1).otherwise(0)).alias('add_to_cart_sessions'), F.sum(F.when(F.col('checkout_sequence').isNotNull(), 1).otherwise(0)).alias('checkout_sessions'), F.sum(F.when(F.col('purchase_sequence').isNotNull(), 1).otherwise(0)).alias('purchase_sessions'))
    daily = daily.withColumn('add_to_cart_rate', F.when(F.col('sessions') > 0, F.col('add_to_cart_sessions') / F.col('sessions')).otherwise(0.0)).withColumn('checkout_rate', F.when(F.col('sessions') > 0, F.col('checkout_sessions') / F.col('sessions')).otherwise(0.0)).withColumn('purchase_rate', F.when(F.col('sessions') > 0, F.col('purchase_sessions') / F.col('sessions')).otherwise(0.0)).withColumn('overall_conversion_rate', F.when(F.col('sessions') > 0, F.col('purchase_sessions') / F.col('sessions')).otherwise(0.0)).orderBy('date')
    return daily

def calculate_shortcut_paths(sessionized_events: DataFrame) -> DataFrame:
    """
    Detect alternate paths from the original sessionized events.

    Shortcut paths are diagnostic results and do not change the
    strict funnel conversion counts.

    Categories:
        1. Product View → Checkout
           Checkout occurred without an earlier Add to Cart and
           no Purchase occurred after that Checkout.

        2. Product View → Checkout → Purchase
           Checkout and a later Purchase occurred without an
           earlier Add to Cart.

        3. Product View → Purchase
           Purchase occurred without an earlier Add to Cart or
           Checkout.
    """
    product_view_sessions = sessionized_events.filter(F.col('event_type') == 'page_view').groupBy('session_id').agg(F.min('event_sequence').alias('product_view_sequence'))
    later_events = sessionized_events.alias('events').join(product_view_sessions.alias('views'), F.col('events.session_id') == F.col('views.session_id'), 'inner').filter(F.col('events.event_sequence') > F.col('views.product_view_sequence')).select(F.col('events.session_id').alias('session_id'), F.col('events.event_type').alias('event_type'), F.col('events.event_sequence').alias('event_sequence'))
    session_paths = later_events.groupBy('session_id').agg(F.min(F.when(F.col('event_type') == 'add_to_cart', F.col('event_sequence'))).alias('add_to_cart_sequence'), F.min(F.when(F.col('event_type') == 'checkout', F.col('event_sequence'))).alias('checkout_sequence'), F.min(F.when(F.col('event_type') == 'purchase', F.col('event_sequence'))).alias('purchase_sequence'))
    checkout_to_purchase = session_paths.filter(F.col('checkout_sequence').isNotNull() & F.col('purchase_sequence').isNotNull() & (F.col('purchase_sequence') > F.col('checkout_sequence')) & (F.col('add_to_cart_sequence').isNull() | (F.col('add_to_cart_sequence') > F.col('checkout_sequence')))).select(F.lit('Product View → Checkout → Purchase').alias('path'), 'session_id')
    checkout_without_purchase = session_paths.filter(F.col('checkout_sequence').isNotNull() & (F.col('add_to_cart_sequence').isNull() | (F.col('add_to_cart_sequence') > F.col('checkout_sequence'))) & (F.col('purchase_sequence').isNull() | (F.col('purchase_sequence') < F.col('checkout_sequence')))).select(F.lit('Product View → Checkout').alias('path'), 'session_id')
    direct_purchase = session_paths.filter(F.col('purchase_sequence').isNotNull() & (F.col('checkout_sequence').isNull() | (F.col('checkout_sequence') > F.col('purchase_sequence'))) & (F.col('add_to_cart_sequence').isNull() | (F.col('add_to_cart_sequence') > F.col('purchase_sequence')))).select(F.lit('Product View → Purchase').alias('path'), 'session_id')
    return checkout_to_purchase.unionByName(checkout_without_purchase).unionByName(direct_purchase).groupBy('path').agg(F.countDistinct('session_id').alias('sessions')).orderBy(F.desc('sessions'), F.asc('path'))

def calculate_segmented_funnel(funnel_sessions: DataFrame, metadata: DataFrame, segment_column: str) -> DataFrame:
    """
    Calculate funnel metrics by:

        source
        device
        country
        or another session-level dimension.
    """
    segment_metadata = metadata.select('session_id', segment_column).dropDuplicates(['session_id'])
    segmented = funnel_sessions.join(segment_metadata, 'session_id', 'left')
    result = segmented.groupBy(segment_column).agg(F.count('*').alias('sessions'), F.sum(F.when(F.col('add_to_cart_sequence').isNotNull(), 1).otherwise(0)).alias('add_to_cart_sessions'), F.sum(F.when(F.col('checkout_sequence').isNotNull(), 1).otherwise(0)).alias('checkout_sessions'), F.sum(F.when(F.col('purchase_sequence').isNotNull(), 1).otherwise(0)).alias('purchase_sessions')).withColumn('add_to_cart_rate', F.when(F.col('sessions') > 0, F.col('add_to_cart_sessions') / F.col('sessions')).otherwise(0.0)).withColumn('checkout_rate', F.when(F.col('sessions') > 0, F.col('checkout_sessions') / F.col('sessions')).otherwise(0.0)).withColumn('purchase_rate', F.when(F.col('sessions') > 0, F.col('purchase_sessions') / F.col('sessions')).otherwise(0.0)).orderBy(F.desc('sessions'))
    return result

def validate_funnel(funnel_sessions: DataFrame, funnel_summary: DataFrame) -> None:
    """
    Validate the strict funnel.
    """
    duplicate_sessions = funnel_sessions.groupBy('session_id').count().filter(F.col('count') > 1).count()
    if duplicate_sessions > 0:
        raise ValueError(f'Funnel validation failed: {duplicate_sessions} duplicate sessions found.')
    missing_product_view = funnel_sessions.filter(F.col('product_view_sequence').isNull()).count()
    if missing_product_view > 0:
        raise ValueError(f'Funnel validation failed: {missing_product_view} sessions missing Product View.')
    chronology_errors = funnel_sessions.filter(F.col('add_to_cart_sequence').isNotNull() & (F.col('add_to_cart_sequence') <= F.col('product_view_sequence')) | F.col('checkout_sequence').isNotNull() & (F.col('checkout_sequence') <= F.col('add_to_cart_sequence')) | F.col('purchase_sequence').isNotNull() & (F.col('purchase_sequence') <= F.col('checkout_sequence'))).count()
    if chronology_errors > 0:
        raise ValueError(f'Funnel validation failed: {chronology_errors} chronology violations found.')
    counts = funnel_sessions.agg(F.count('*').alias('product_view'), F.sum(F.when(F.col('add_to_cart_sequence').isNotNull(), 1).otherwise(0)).alias('add_to_cart'), F.sum(F.when(F.col('checkout_sequence').isNotNull(), 1).otherwise(0)).alias('checkout'), F.sum(F.when(F.col('purchase_sequence').isNotNull(), 1).otherwise(0)).alias('purchase')).first()
    product_view = int(counts['product_view'])
    add_to_cart = int(counts['add_to_cart'])
    checkout = int(counts['checkout'])
    purchase = int(counts['purchase'])
    if not product_view >= add_to_cart >= checkout >= purchase:
        raise ValueError('Funnel validation failed: stage counts are not monotonic.')
    invalid_rates = funnel_summary.filter((F.col('conversion_rate') < 0) | (F.col('conversion_rate') > 1) | (F.col('dropoff_rate') < 0) | (F.col('dropoff_rate') > 1)).count()
    if invalid_rates > 0:
        raise ValueError('Funnel validation failed: invalid conversion/drop-off rates found.')
    stage_count = funnel_summary.count()
    if stage_count != 4:
        raise ValueError(f'Funnel validation failed: expected 4 stages, found {stage_count}.')

def run_funnel(sessionized_events: DataFrame):
    """
    Run the complete Funnel Analysis pipeline.

    Returns a dictionary because funnel_test.py and later
    FastAPI/database integration use named result objects.

    Keys:

        funnel_sessions
        funnel_summary
        funnel_time_metrics
        funnel_trends
        funnel_shortcuts
    """
    spark = sessionized_events.sparkSession
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)
    funnel_sessions = build_funnel_sessions(sessionized_events).cache()
    funnel_session_count = funnel_sessions.count()
    funnel_summary = calculate_funnel_summary(funnel_sessions)
    funnel_time_metrics = calculate_funnel_time_metrics(funnel_sessions)
    funnel_trends = calculate_funnel_trends(funnel_sessions)
    funnel_shortcuts = calculate_shortcut_paths(sessionized_events).cache()
    shortcut_count = funnel_shortcuts.count()
    if shortcut_count > 0:
        pass
    validate_funnel(funnel_sessions, funnel_summary)
    pass
    pass
    pass
    return {'funnel_sessions': funnel_sessions, 'funnel_summary': funnel_summary, 'funnel_time_metrics': funnel_time_metrics, 'funnel_trends': funnel_trends, 'funnel_shortcuts': funnel_shortcuts}
