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
- **Salespeople** (`dim_salespeople`): Salesperson performance and territory
- **Sales Reasons** (`dim_sales_reasons`): Sales motivation and categorization
- **Credit Cards** (`dim_credit_cards`): Payment method information
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

## Getting Started

### Prerequisites

- DBT installed and configured
- Access to Adventure Works database (fea24_11.raw_adventure_works)
- Snowflake connection configured

### Installation

1. Clone the repository
2. Install dependencies: `dbt deps`
3. Configure your profile in `~/.dbt/profiles.yml`
4. Run the models: `dbt run`
5. Run tests: `dbt test`

### Key Commands

```bash
# Run all models
dbt run

# Run specific models
dbt run --select marts

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run custom test for 2011 sales validation
dbt run-operation custom_test_2011_gross_sales
```

## Data Quality & Testing

### Automated Tests

- **Primary Key Tests**: Ensures uniqueness of dimension keys
- **Not Null Tests**: Validates required fields
- **Referential Integrity**: Ensures foreign key relationships
- **Custom Tests**: Validates business rules (e.g., 2011 sales amount)

### Data Validation

The project includes comprehensive testing to ensure:
- Data completeness and accuracy
- Business rule compliance
- Referential integrity
- Performance optimization

## Performance Considerations

- **Materialization Strategy**: Marts are materialized as tables for query performance
- **Indexing**: Surrogate keys are optimized for joins
- **Partitioning**: Date-based partitioning for time series analysis
- **Aggregation**: Pre-calculated metrics for common business questions

## Contributing

1. Follow the established naming conventions
2. Use CTEs instead of subqueries
3. Include comprehensive documentation
4. Add appropriate tests for new models
5. Follow the three-layer transformation approach

## Support

For questions or issues, please refer to:
- DBT documentation: https://docs.getdbt.com/
- Adventure Works schema: https://dataedo.com/samples/html/AdventureWorks/doc/AdventureWorks_2/home.html
- Project-specific rules in `.cursor/rules/dbt-rules.mdc`
