# 🎯 dbt Supabase Migration Guide

## 📋 Overview
This guide explains how to migrate your dbt models from PostgreSQL to Supabase.

## 🔧 Key Changes Required

### 1. **dbt Profiles Configuration**
Your dbt profiles need to be updated to point to Supabase instead of your current PostgreSQL database.

**File**: `profiles_supabase.yml` (created)
**Location**: `~/.dbt/profiles.yml` (Windows: `C:\Users\<username>\.dbt\profiles.yml`)

### 2. **Environment Variables**
Add these to your `.env` file:
```bash
# Supabase dbt connection
SUPABASE_HOST=your-project-id.supabase.co
SUPABASE_USER=postgres
SUPABASE_PASSWORD=your-service-role-key
SUPABASE_PORT=5432
SUPABASE_DBNAME=postgres
SUPABASE_SCHEMA=wnba
```

### 3. **Schema Creation**
The `wnba` schema needs to be created in Supabase before running dbt models.

## 🚀 Migration Steps

### Step 1: Setup dbt for Supabase
```bash
# Run the setup script
py setup_dbt_supabase.py
```

This script will:
- ✅ Create dbt profiles configuration
- ✅ Create the `wnba` schema in Supabase
- ✅ Test dbt connection
- ✅ Optionally run dbt models

### Step 2: Verify dbt Configuration
```bash
cd wnba_dbt
dbt debug
```

You should see:
```
✅ All checks passed!
```

### Step 3: Run dbt Models
```bash
cd wnba_dbt
dbt run
```

This will create all your tables in the Supabase `wnba` schema.

## 📊 What Gets Created

Your dbt models will create these tables in Supabase:

### **Staging Tables**
- Raw data from your data sources
- Temporary tables for data processing

### **Intermediate Tables**
- Processed data with business logic applied
- Aggregations and calculations

### **Mart Tables**
- Final presentation layer
- Optimized for reporting and analysis

### **Analysis Tables**
- Specialized analysis tables
- Custom aggregations and insights

## 🔍 Verification

After running dbt, verify the tables were created:

```sql
-- Check tables in wnba schema
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'wnba'
ORDER BY table_name;
```

## ⚠️ Important Notes

### **1. Data Source Changes**
Your dbt models currently read from your existing PostgreSQL tables. You'll need to update the source configurations to point to Supabase tables.

### **2. Schema References**
Update any hardcoded schema references in your dbt models from your current schema to `wnba`.

### **3. Permissions**
Ensure your Supabase service role key has the necessary permissions to:
- Create schemas
- Create tables
- Insert/update data
- Execute SQL

### **4. Performance**
Supabase may have different performance characteristics than your current PostgreSQL setup. Monitor query performance and optimize if needed.

## 🔄 Migration Process

### **Phase 1: Setup (Complete)**
- ✅ Created `profiles_supabase.yml`
- ✅ Created `setup_dbt_supabase.py`
- ✅ Updated TODO list

### **Phase 2: Execution**
- [ ] Run setup script
- [ ] Test dbt connection
- [ ] Run dbt models
- [ ] Verify table creation

### **Phase 3: Validation**
- [ ] Test data pipeline with new tables
- [ ] Verify data integrity
- [ ] Performance testing
- [ ] Update source configurations

## 🛠️ Troubleshooting

### **Common Issues**

1. **Connection Failed**
   - Check environment variables
   - Verify Supabase credentials
   - Ensure SSL is enabled

2. **Schema Not Found**
   - Run the setup script to create schema
   - Check permissions

3. **Models Failed**
   - Check source table references
   - Verify data exists in source tables
   - Review error logs

### **Useful Commands**

```bash
# Test connection
dbt debug

# Run specific models
dbt run --select model_name

# Run with verbose output
dbt run --verbose

# Check model status
dbt ls

# Generate documentation
dbt docs generate
```

## 📈 Benefits of Supabase + dbt

### **1. Real-time Features**
- Real-time subscriptions to table changes
- Live updates for dashboards

### **2. Built-in APIs**
- Automatic REST API generation
- GraphQL support

### **3. Authentication**
- Built-in user authentication
- Row-level security

### **4. Storage Integration**
- File storage alongside database
- Unified data platform

## 🎯 Next Steps

1. **Run the setup script**: `py setup_dbt_supabase.py`
2. **Test the connection**: `cd wnba_dbt && dbt debug`
3. **Run your models**: `dbt run`
4. **Update your data pipeline** to use Supabase tables
5. **Test end-to-end functionality**

---

*This guide covers the dbt-specific changes needed for Supabase migration. For the complete migration process, refer to the main TODO list.*
