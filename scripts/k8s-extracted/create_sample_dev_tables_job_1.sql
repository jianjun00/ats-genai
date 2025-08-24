-- SQL Script

# Wait for postgres to be ready
      until pg_isready -h postgres.ats-dev.svc.cluster.local -p 5432 -U postgres; do
        echo "Waiting for postgres..."
        sleep 2
      done
      
      # Create sample dev_ tables with data
      PGPASSWORD=postgres psql -h postgres.ats-dev.svc.cluster.local -U postgres -d ats_dev << 'EOF'
      
      -- Create dev_users table
      CREATE TABLE dev_users (
          id SERIAL PRIMARY KEY,
          username VARCHAR(50) NOT NULL,
          email VARCHAR(100),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      
      -- Create dev_products table  
      CREATE TABLE dev_products (
          id SERIAL PRIMARY KEY,
          name VARCHAR(100) NOT NULL,
          price DECIMAL(10,2),
          category_id INTEGER,
          description TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      
      -- Create dev_orders table
      CREATE TABLE dev_orders (
          id SERIAL PRIMARY KEY,
          user_id INTEGER REFERENCES dev_users(id),
          product_id INTEGER REFERENCES dev_products(id),
          quantity INTEGER DEFAULT 1,
          total_amount DECIMAL(12,2),
          order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          status VARCHAR(20) DEFAULT 'pending'
      );
      
      -- Insert sample data
      INSERT INTO dev_users (username, email) VALUES
          ('john_doe', 'john@example.com'),
          ('jane_smith', 'jane@example.com'),
          ('bob_wilson', 'bob@example.com');
      
      INSERT INTO dev_products (name, price, category_id, description) VALUES
          ('Laptop Pro', 1299.99, 1, 'High-performance laptop'),
          ('Wireless Mouse', 29.99, 2, 'Ergonomic wireless mouse'),
          ('Mechanical Keyboard', 149.99, 2, 'RGB mechanical keyboard');
      
      INSERT INTO dev_orders (user_id, product_id, quantity, total_amount, status) VALUES
          (1, 1, 1, 1299.99, 'completed'),
          (2, 2, 2, 59.98, 'pending'),
          (3, 3, 1, 149.99, 'shipped'),
          (1, 2, 1, 29.99, 'completed');
      
      -- Show created tables
      \dt+ dev_*
      
      -- Show row counts
      SELECT 'dev_users' as table_name, COUNT(*) as row_count FROM dev_users
      UNION ALL
      SELECT 'dev_products', COUNT(*) FROM dev_products  
      UNION ALL
      SELECT 'dev_orders', COUNT(*) FROM dev_orders;
      
      EOF
      
      echo "✅ Sample dev_ tables created successfully!"
    resources:
      requests:
        memory: "64Mi"
        cpu: "50m"
      limits:
        memory: "128Mi"
        cpu: "100m"
  restartPolicy: Never
backoffLimit: 2
