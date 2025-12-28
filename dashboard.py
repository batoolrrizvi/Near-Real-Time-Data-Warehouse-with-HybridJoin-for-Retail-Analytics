import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# CONFIG
st.set_page_config(
    page_title="Retail Sales Analytics Dashboard",
    layout="wide"
)

st.title("Retail Data Warehouse")
st.caption("Retail Sales Data Analytics")

# DATABASE CONNECTION
@st.cache_resource
def get_engine():
    return create_engine(
        "postgresql+psycopg2://user:password@host:port/walmartDW"
    )

engine = get_engine()

# SIDEBAR FILTERS
st.sidebar.header("Global Filters")

year = st.sidebar.selectbox(
    "Select Year",
    [2017, 2018, 2019],
    index=0
)

view = st.sidebar.radio(
    "Select Query",
    [
        "Top Products (Weekday vs Weekend)",
        "Customer Demographics",
        "Product Category by Occupation",
        "Quarterly Gender & Age Trends",
        "Top Cities by Product Category",
        "Monthly Growth by Product Category"
    ]
)

# QUERIES
def run_query(sql):
    return pd.read_sql(sql, engine)

# dashboard views

if view == "Top Products (Weekday vs Weekend)":
    st.subheader("Top 5 Revenue-Generating Products (Monthly)")

    sql = f"""
    WITH base AS (
        SELECT
            p.product_id,
            p.product_category,
            d.monthNum,
            d.is_weekend,
            SUM(s.sales_amount) AS revenue
        FROM walmartdw.sales s
        JOIN walmartdw.product p ON s.product_id = p.product_id
        JOIN walmartdw.date d ON s.date_id = d.date_id
        WHERE d.year = {year}
        GROUP BY p.product_id, p.product_category, d.monthNum, d.is_weekend
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY monthNum, is_weekend
                ORDER BY revenue DESC
            ) AS rn
        FROM base
    )
    SELECT * FROM ranked WHERE rn <= 5
    ORDER BY monthNum, is_weekend, revenue DESC;
    """

    df = run_query(sql)

    fig = px.bar(
        df,
        x="product_category",
        y="revenue",
        color="is_weekend",
        facet_col="monthnum",
        title="Top Products by Month (Weekday vs Weekend)"
    )

    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------

elif view == "Customer Demographics":
    st.subheader("Customer Demographics by Purchase Amount")

    sql = """
    SELECT
        c.gender,
        c.age_group,
        c.city_category,
        SUM(s.sales_amount) AS total_revenue
    FROM walmartdw.sales s
    JOIN walmartdw.customer c ON s.customer_id = c.customer_id
    GROUP BY c.gender, c.age_group, c.city_category
    ORDER BY c.city_category, c.gender, c.age_group;
    """

    df = run_query(sql)

    fig = px.sunburst(
        df,
        path=["city_category", "gender", "age_group"],
        values="total_revenue",
        title="Revenue Distribution by Demographics"
    )

    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------

elif view == "Product Category by Occupation":
    st.subheader("Product Category Sales by Occupation")

    sql = """
    SELECT
        p.product_category,
        c.occupation,
        SUM(s.sales_amount) AS total_revenue
    FROM walmartdw.sales s
    JOIN walmartdw.product p ON s.product_id = p.product_id
    JOIN walmartdw.customer c ON s.customer_id = c.customer_id
    GROUP BY p.product_category, c.occupation;
    """

    df = run_query(sql)

    fig = px.treemap(
        df,
        path=["product_category", "occupation"],
        values="total_revenue",
        title="Revenue Contribution by Occupation"
    )

    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------

elif view == "Quarterly Gender & Age Trends":
    st.subheader("Quarterly Revenue Trends by Gender and Age Group")

    sql = f"""
    SELECT
        d.quarter_num,
        c.gender,
        c.age_group,
        SUM(s.sales_amount) AS total_revenue
    FROM walmartdw.sales s
    JOIN walmartdw.date d ON s.date_id = d.date_id
    JOIN walmartdw.customer c ON s.customer_id = c.customer_id
    WHERE d.year = {year}
    GROUP BY d.quarter_num, c.gender, c.age_group
    ORDER BY d.quarter_num;
    """

    df = run_query(sql)

    fig = px.line(
    df,
    x="quarter_num",
    y="total_revenue",
    color="gender",
    line_dash="age_group",
    markers=True,
    title="Quarterly Revenue Trends",
    color_discrete_map={
        "M": "blue",
        "F": "pink"
    }
)

    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------

elif view == "Top Cities by Product Category":
    st.subheader("Top 5 Cities by Product Category Revenue")

    sql = """
    WITH city_rev AS (
        SELECT
            c.city_category,
            p.product_category,
            SUM(s.sales_amount) AS total_revenue
        FROM walmartdw.sales s
        JOIN walmartdw.customer c ON s.customer_id = c.customer_id
        JOIN walmartdw.product p ON s.product_id = p.product_id
        GROUP BY c.city_category, p.product_category
    )
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY product_category
                ORDER BY total_revenue DESC
            ) AS rn
        FROM city_rev
    ) t WHERE rn <= 5;
    """

    df = run_query(sql)

    fig = px.bar(
        df,
        x="city_category",
        y="total_revenue",
        color="product_category",
        title="Top Cities by Product Category"
    )

    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------

elif view == "Monthly Growth by Product Category":
    st.subheader("Month-over-Month Growth (%)")

    sql = f"""
    WITH monthly AS (
        SELECT
            p.product_category,
            d.monthNum,
            SUM(s.sales_amount) AS revenue
        FROM walmartdw.sales s
        JOIN walmartdw.date d ON s.date_id = d.date_id
        JOIN walmartdw.product p ON s.product_id = p.product_id
        WHERE d.year = {year}
        GROUP BY p.product_category, d.monthNum
    )
    SELECT
        product_category,
        monthNum,
        ROUND(
            (revenue - LAG(revenue) OVER (
                PARTITION BY product_category ORDER BY monthNum
            ))
            / NULLIF(LAG(revenue) OVER (
                PARTITION BY product_category ORDER BY monthNum
            ), 0) * 100, 2
        ) AS growth_percent
    FROM monthly;
    """

    df = run_query(sql)

    fig = px.line(
        df,
        x="monthnum",
        y="growth_percent",
        color="product_category",
        markers=True,
        title="Monthly Sales Growth %"
    )

    st.plotly_chart(fig, use_container_width=True)


# RAW DATA INSPECTOR
with st.expander("🔍 View Raw Data"):
    st.dataframe(df)
