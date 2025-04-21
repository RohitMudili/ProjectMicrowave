import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from db_utils import DatabaseManager
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Initialize database manager
db = DatabaseManager()

# Set page config
st.set_page_config(
    page_title="Farm Customers Dashboard",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title(" Organic Farm Customer Dashboard")
st.markdown("""
This dashboard provides insights into customer data, sales, and product performance.
Use the sidebar to navigate between different views and filter data.
""")

# Sidebar navigation
page = st.sidebar.selectbox(
    "Choose a View",
    ["Customer Overview", "Sales Analysis", "Product Performance", "Customer Search", "Customer Segmentation", "Product Recommendations"]
)

# Date filter in sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("Date Filter")
date_filter = st.sidebar.radio(
    "Filter by date range:",
    ["All Time", "Last 30 Days", "Last 90 Days", "Last Year", "Custom Range"]
)

# Custom date range if selected
start_date = None
end_date = None
if date_filter == "Custom Range":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", datetime.now())
elif date_filter == "Last 30 Days":
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
elif date_filter == "Last 90 Days":
    start_date = datetime.now() - timedelta(days=90)
    end_date = datetime.now()
elif date_filter == "Last Year":
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

# Set date filter in database manager
db.set_date_filter(start_date, end_date)

# Customer type filter
st.sidebar.markdown("---")
st.sidebar.subheader("Customer Type Filter")
customer_type = st.sidebar.multiselect(
    "Filter by customer type:",
    ["All", "Only Once", "Frequent", "Rare"],
    default=["All"]
)

if page == "Customer Overview":
    st.header("Customer Overview")
    
    # Get top customers
    top_customers = db.get_top_customers(10)
    
    # Create three columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Top 10 Customers by Spending")
        fig = px.bar(
            top_customers,
            x='first_name',
            y='total_spent',
            title='Top Customers by Total Spent',
            labels={'first_name': 'Customer Name', 'total_spent': 'Total Spent ($)'},
            color='total_spent',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Customer Order Distribution")
        fig = px.histogram(
            top_customers,
            x='total_orders',
            title='Distribution of Orders per Customer',
            labels={'total_orders': 'Number of Orders'},
            nbins=10,
            color_discrete_sequence=['#1f77b4']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        st.subheader("Customer Metrics")
        total_customers = len(top_customers)
        avg_spent = top_customers['total_spent'].mean()
        avg_orders = top_customers['total_orders'].mean()
        
        st.metric("Total Customers", total_customers)
        st.metric("Average Spent", f"${avg_spent:.2f}")
        st.metric("Average Orders", f"{avg_orders:.1f}")
    
    # Display raw data with search
    st.subheader("Customer Details")
    search_term = st.text_input("Search customers by name or email:")
    if search_term:
        filtered_customers = db.search_customers(search_term)
        st.dataframe(filtered_customers)
    else:
        st.dataframe(top_customers)

elif page == "Sales Analysis":
    st.header("Sales Analysis")
    
    # Get sales by category
    category_sales = db.get_sales_by_category()
    
    # Create two columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Sales by Category")
        fig = px.pie(
            category_sales,
            values='total_revenue',
            names='category',
            title='Revenue Distribution by Category',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Orders by Category")
        fig = px.bar(
            category_sales,
            x='category',
            y='total_orders',
            title='Number of Orders by Category',
            color='total_revenue',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Sales trends
    st.subheader("Sales Trends")
    sales_trends = db.get_sales_trends()
    if not sales_trends.empty:
        fig = px.line(
            sales_trends,
            x='date',
            y='revenue',
            title='Daily Sales Revenue',
            labels={'date': 'Date', 'revenue': 'Revenue ($)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Display raw data
    st.subheader("Category Sales Details")
    st.dataframe(category_sales)

elif page == "Product Performance":
    st.header("Product Performance")
    
    # Get product sales
    product_sales = db.get_product_sales()
    
    # Create two columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Products by Revenue")
        fig = px.bar(
            product_sales,
            x='name',
            y='total_revenue',
            title='Product Revenue',
            labels={'name': 'Product Name', 'total_revenue': 'Total Revenue ($)'},
            color='total_revenue',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Product Quantity Sold")
        fig = px.bar(
            product_sales,
            x='name',
            y='total_quantity',
            title='Quantity Sold by Product',
            labels={'name': 'Product Name', 'total_quantity': 'Total Quantity'},
            color='total_quantity',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Product metrics
    st.subheader("Product Metrics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Products", len(product_sales))
    with col2:
        st.metric("Total Revenue", f"${product_sales['total_revenue'].sum():.2f}")
    with col3:
        st.metric("Total Quantity", product_sales['total_quantity'].sum())
    
    # Display raw data
    st.subheader("Product Sales Details")
    st.dataframe(product_sales)

elif page == "Customer Search":
    st.header("Customer Search")
    
    # Advanced search options
    col1, col2 = st.columns(2)
    with col1:
        search_term = st.text_input("Search customers by name or email:")
    with col2:
        search_type = st.selectbox(
            "Search by:",
            ["Name", "Email", "Phone", "Address", "City", "State"]
        )
    
    if search_term:
        # Get search results
        results = db.search_customers(search_term)
        
        if not results.empty:
            st.subheader("Search Results")
            
            # Display customer details
            for _, customer in results.iterrows():
                with st.expander(f"{customer['first_name']} {customer['last_name']} - {customer['email']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Phone:** {customer['phone']}")
                        st.write(f"**Address:** {customer['address']}")
                        st.write(f"**City:** {customer['city']}")
                    with col2:
                        st.write(f"**State:** {customer['state']}")
                        st.write(f"**Zip Code:** {customer['zip_code']}")
                        st.write(f"**Customer ID:** {customer['customer_id']}")
                    
                    # Get customer orders
                    orders = db.get_customer_orders(customer['customer_id'])
                    if not orders.empty:
                        st.write("**Order History:**")
                        st.dataframe(orders)
                        
                        # Customer order summary
                        st.write("**Order Summary:**")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Orders", len(orders))
                        with col2:
                            st.metric("Total Spent", f"${orders['total_amount'].sum():.2f}")
                        with col3:
                            st.metric("Average Order Value", f"${orders['total_amount'].mean():.2f}")
                    else:
                        st.write("No orders found for this customer.")
        else:
            st.info("No customers found matching your search criteria.")

elif page == "Customer Segmentation":
    st.header("Customer Segmentation")
    
    # Get customer data for segmentation
    customers = db.get_all_customers()
    
    if not customers.empty:
        # RFM Analysis
        st.subheader("RFM Customer Segmentation")
        
        # Calculate RFM metrics
        customers['recency'] = (pd.Timestamp.now() - pd.to_datetime(customers['last_order_date'])).dt.days
        customers['frequency'] = customers['total_orders']
        customers['monetary'] = customers['total_spent']
        
        # Create RFM score - using a simpler approach to avoid the qcut error
        def calculate_rfm_score(row):
            # Recency score (1-5)
            if pd.isna(row['recency']) or row['recency'] > 365:
                recency_score = 1
            elif row['recency'] > 180:
                recency_score = 2
            elif row['recency'] > 90:
                recency_score = 3
            elif row['recency'] > 30:
                recency_score = 4
            else:
                recency_score = 5
                
            # Frequency score (1-5)
            if pd.isna(row['frequency']) or row['frequency'] == 0:
                frequency_score = 1
            elif row['frequency'] == 1:
                frequency_score = 2
            elif row['frequency'] <= 3:
                frequency_score = 3
            elif row['frequency'] <= 5:
                frequency_score = 4
            else:
                frequency_score = 5
                
            # Monetary score (1-5)
            if pd.isna(row['monetary']) or row['monetary'] == 0:
                monetary_score = 1
            elif row['monetary'] < 100:
                monetary_score = 2
            elif row['monetary'] < 500:
                monetary_score = 3
            elif row['monetary'] < 1000:
                monetary_score = 4
            else:
                monetary_score = 5
                
            return recency_score + frequency_score + monetary_score
        
        customers['rfm_score'] = customers.apply(calculate_rfm_score, axis=1)
        
        # Segment customers
        def segment_customer(row):
            if row['rfm_score'] >= 13:
                return 'Champions'
            elif row['rfm_score'] >= 10:
                return 'Loyal Customers'
            elif row['rfm_score'] >= 7:
                return 'At Risk'
            else:
                return 'Lost'
        
        customers['segment'] = customers.apply(segment_customer, axis=1)
        
        # Display segmentation results
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                customers,
                names='segment',
                title='Customer Segments',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            segment_stats = customers.groupby('segment').agg({
                'customer_id': 'count',
                'total_spent': 'mean',
                'total_orders': 'mean'
            }).reset_index()
            
            segment_stats.columns = ['Segment', 'Count', 'Avg Spent', 'Avg Orders']
            st.dataframe(segment_stats)
        
        # Customer details by segment
        st.subheader("Customer Details by Segment")
        segment_filter = st.selectbox("Select Segment:", customers['segment'].unique())
        filtered_customers = customers[customers['segment'] == segment_filter]
        st.dataframe(filtered_customers[['first_name', 'last_name', 'email', 'total_orders', 'total_spent', 'rfm_score']])

elif page == "Product Recommendations":
    st.header("Product Recommendations")
    
    # Get product data
    products = db.get_all_products()
    orders = db.get_all_orders()
    
    if not products.empty and not orders.empty:
        # Product popularity
        st.subheader("Product Popularity")
        
        # Calculate product popularity
        product_popularity = orders.groupby('product_id').agg({
            'order_id': 'count',
            'quantity': 'sum'
        }).reset_index()
        
        product_popularity.columns = ['product_id', 'order_count', 'total_quantity']
        
        # Merge with product details
        product_popularity = product_popularity.merge(products, on='product_id')
        
        # Display popular products
        fig = px.bar(
            product_popularity,
            x='name',
            y='order_count',
            title='Most Popular Products',
            labels={'name': 'Product Name', 'order_count': 'Number of Orders'},
            color='order_count',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Product recommendations
        st.subheader("Product Recommendations")
        
        # Simple recommendation based on category
        product_categories = products['category'].unique()
        selected_category = st.selectbox("Select a category:", product_categories)
        
        recommended_products = products[products['category'] == selected_category]
        
        if not recommended_products.empty:
            st.write(f"Recommended products in the {selected_category} category:")
            st.dataframe(recommended_products[['name', 'description', 'price']])
        else:
            st.info(f"No products found in the {selected_category} category.")
        
        # Customer-specific recommendations
        st.subheader("Customer-Specific Recommendations")
        customer_id = st.text_input("Enter Customer ID:")
        
        if customer_id:
            customer_orders = db.get_customer_orders(customer_id)
            
            if not customer_orders.empty:
                # Get customer's purchase history
                purchased_products = customer_orders['product_id'].unique()
                
                # Find products in the same category that the customer hasn't purchased
                customer_categories = products[products['product_id'].isin(purchased_products)]['category'].unique()
                recommended_products = products[
                    (products['category'].isin(customer_categories)) & 
                    (~products['product_id'].isin(purchased_products))
                ]
                
                if not recommended_products.empty:
                    st.write("Recommended products based on your purchase history:")
                    st.dataframe(recommended_products[['name', 'description', 'price', 'category']])
                else:
                    st.info("No new recommendations available based on your purchase history.")
            else:
                st.info("No order history found for this customer.") 