# Adventure Works Data Warehouse

## Project Overview

This project implements a comprehensive data warehouse for Adventure Works, a manufacturing company, using DBT (Data Build Tool). The data warehouse is designed to answer critical business questions about sales performance, customer behavior, and product performance.

## Business Context

Carlos, the project manager, has provided a key validation requirement:
- **2011 Gross Sales**: $12,646,112.16

This value is used as a custom test to ensure data accuracy in the models.

## Business Questions Addressed

### 1. Sales Metrics by Dimensions
**Question**: What is the number of orders, quantity purchased, and total negotiated value by product, credit card type, sales reason, sale date, customer, status, city, state, and country?

**Analysis**: `analyses/adventure_works/01_sales_by_dimensions.sql`

### 2. Average Ticket Analysis
**Question**: Which products have the highest average ticket by month, year, city, state, and country? (Average ticket = Gross revenue - discounts / number of orders)

**Analysis**: `analyses/adventure_works/02_average_ticket_by_dimensions.sql`

### 3. Top 10 Customers by Value
**Question**: Who are the top 10 customers by total negotiated value, filtered by product, credit card type, sales reason, sale date, status, city, state, and country?

**Analysis**: `analyses/adventure_works/03_top_10_customers_by_value.sql`

### 4. Top 5 Cities by Value
**Question**: What are the top 5 cities by total negotiated value, filtered by product, credit card type, sales reason, sale date, customer, status, city, state, and country?

**Analysis**: `analyses/adventure_works/04_top_5_cities_by_value.sql`

### 5. Time Series Analysis
**Question**: What is the number of orders, quantity purchased, and total negotiated value by month and year? (Hint: time series chart)

**Analysis**: `analyses/adventure_works/05_time_series_analysis.sql`

### 6. Promotion Sales Analysis
**Question**: Which product has the highest quantity of units purchased for the "Promotion" sales reason?

**Analysis**: `analyses/adventure_works/06_products_by_promotion_sales.sql`

## Data Architecture

### Three-Layer Transformation Approach

1. **Staging Layer** (`models/staging/`)
   - Raw data extraction and basic cleaning
   - Source-specific transformations
   - Data type casting and null handling

2. **Intermediate Layer** (`models/intermediate/`)
   - Business logic implementation
   - Dimension table preparation
   - Complex joins and calculations

3. **Mart Layer** (`models/marts/`)
   - Final dimensional model
   - Optimized for business analysis
   - Materialized as tables for performance

### Key Dimensions

- **Customers** (`dim_customers`): Customer information, location, territory
- **Products** (`dim_products`): Product hierarchy, categories, subcategories
- **Sales Reasons** (`dim_sales_reasons`): Sales motivation and categorization
- **Dates** (`dim_dates`): Time dimension for trend analysis

### Fact Table

- **Sales** (`fact_sales`): Comprehensive sales transactions with all metrics

## Data Dictionary

### Key Metrics

| Metric | Calculation | Primary Table |
|--------|-------------|---------------|
| Number of Orders | Distinct count of salesorderid | Sales Order Header |
| Quantity Purchased | Sum of orderqty | Sales Order Detail |
| Gross Value | Sum of [unitprice * orderqty] | Sales Order Detail |
| Net Value | Sum of [unitprice * orderqty * (1 - discount)] | Sales Order Detail |

### Key Dimensions

| Dimension | Database Column | Tables |
|-----------|----------------|---------|
| Customer | person.Name | Customer/Person |
| Sales Reason | salesreason.Name | Sales Reason |
| Salesperson | person.Name | Salesperson/Employee/Person |
| State | Stateprovince.Name | State Province |
| City | Address.City | Address |
| Country | Countryregion.Name | Country Region |
| Sale Date | Salesorderheader.orderdate | Sales Order Header |
| Product | Product.name | Sales Order Detail/Product |

## Project Structure

```
ae_certification_indicium/
├── models/
│   ├── staging/
│   │   └── adventure_works/
│   │       ├── person/
│   │       ├── products/
│   │       ├── sales/
│   │       └── location/
│   ├── intermediate/
│   │   └── adventure_works/
│   └── marts/
│       └── adventure_works/
├── analyses/
│   └── adventure_works/
├── tests/
├── macros/
└── seeds/
```