# Adventure Works Data Warehouse (Certificado Indicium AE)

## Project Overview

This project implements a comprehensive data warehouse for Adventure Works, a manufacturing company, using DBT (Data Build Tool). The data warehouse is designed to answer critical business questions about sales performance, customer behavior, and product performance.

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