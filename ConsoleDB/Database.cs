using System;
using Npgsql;
using Dapper;
using ConsoleDB;
using System.Collections.Generic;
public class DatabaseConnector
{
    private readonly string _connectionString;
    private Random _random = new Random();

    public DatabaseConnector(string connectionString)
    {
        _connectionString = connectionString;
    }
    
    public void SetDefaultSchema(string schema)
    {
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            conn.Open();
            using (var cmd = new NpgsqlCommand($"SET search_path TO {schema};", conn))
            {
                cmd.ExecuteNonQuery();
            }
            
        }
    }

    public void TestConnection()
    {
        try
        {
            using (var conn = new NpgsqlConnection(_connectionString))
            {
                conn.Open();
                Console.WriteLine("Connexion à la base de données réussie.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Une erreur est survenue lors de la connexion à la base de données: {ex.Message}");
        }
    }
    public void InitializeDatabaseTables()
    {
        var createTablesQuery = @"
                CREATE TABLE IF NOT EXISTS Users (
                    user_id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    surname VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS Products (
                    product_id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price DECIMAL(10, 2) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS Purchases (
                    purchase_id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    product_id INT NOT NULL,
                    quantity INT NOT NULL,
                    purchase_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES Users(user_id),
                    FOREIGN KEY (product_id) REFERENCES Products(product_id)
                );

                CREATE TABLE IF NOT EXISTS Subscriptions (
                    subscription_id SERIAL PRIMARY KEY,
                    follower_id INT NOT NULL,
                    followed_id INT NOT NULL,
                    subscription_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (follower_id) REFERENCES Users(user_id),
                    FOREIGN KEY (followed_id) REFERENCES Users(user_id),
                    UNIQUE (follower_id, followed_id)
                );";

        using (var conn = new NpgsqlConnection(_connectionString))
        {
            conn.Open();
            using (var cmd = new NpgsqlCommand(createTablesQuery, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
    public void InsertProducts(int n)
    {
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            conn.Open();

            for (int i = 0; i < n; i++)
            {
                // Génération des informations du produit
                string productName = $"Product {i + 1}";
                decimal productPrice = _random.Next(1, 1000) + _random.Next(0, 100) / 100m;

                string insertQuery = "INSERT INTO Products (name, price) VALUES (@Name, @Price);";

                using (var cmd = new NpgsqlCommand(insertQuery, conn))
                {
                    cmd.Parameters.AddWithValue("@Name", productName);
                    cmd.Parameters.AddWithValue("@Price", productPrice);
                    cmd.ExecuteNonQuery();
                }
            }
        }
        Console.WriteLine($"{n} products have been inserted.");
    }
    
    public (int firstUserId, int lastUserId) GenerateUsers(int n)
    {
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            conn.Open();
            int lastUserIdBeforeInsert = GetLastUserId(conn);
            int firstNewUserId = lastUserIdBeforeInsert + 1; 
            int lastNewUserId = lastUserIdBeforeInsert; 
            int productCount = 10000;
            
            for (int i = 0; i <= n; i++)
            {
                int userIdToInsert = firstNewUserId + i; 
                string name = $"name{userIdToInsert}";
                string surname = $"surname{userIdToInsert}";
                var new_user_id = InsertNewUser(conn, name, surname);
                lastNewUserId = new_user_id;
            }
            return (firstNewUserId, lastNewUserId);
        }
    }
    public void GenerateDataForAllUsers(int firstUserId, int lastUserId)
    {
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            conn.Open();
            var userIds = GetAllUserIds(conn, firstUserId, lastUserId);
            foreach (var userId in userIds)
            {
                int productCount = GetCount(conn, "Products");
                int userCount = userIds.Count;
                GeneratePurchasesForUser(conn, userId, productCount);
                GenerateFollowsForUser(conn, userId, userCount);
            }
        }
    }
    private List<int> GetAllUserIds(NpgsqlConnection conn, int firstUserId, int lastUserId)
    {
        var userIds = new List<int>();
        using (var cmd = new NpgsqlCommand($"SELECT user_id FROM Users WHERE user_id >= @FirstUserId AND user_id <= @LastUserId ORDER BY user_id ASC;", conn))
        {
            cmd.Parameters.AddWithValue("@FirstUserId", firstUserId);
            cmd.Parameters.AddWithValue("@LastUserId", lastUserId);
        
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    userIds.Add(reader.GetInt32(0)); // 0 est l'index de la colonne user_id
                }
            }
        }
        return userIds;
    }
    private int GetLastUserId(NpgsqlConnection conn)
    {
        var query = "SELECT COALESCE(MAX(user_id), 0) FROM Users;";
        using (var cmd = new NpgsqlCommand(query, conn))
        {
            return (int)cmd.ExecuteScalar();
        }
    }
    
    private int InsertNewUser(NpgsqlConnection conn, string name, string surname)
    {
        
        var query = @"
            INSERT INTO Users (name, surname) VALUES (@Name, @Surname) RETURNING user_id;";

        var parameters = new { Name = name, Surname = surname };
        int newUserId = conn.QuerySingle<int>(query, parameters);
        return newUserId;
    }

    private void GeneratePurchasesForUser(NpgsqlConnection conn, int userId, int productCount)
    {
        int numberOfPurchases = _random.Next(0, 6);

        for (int i = 0; i < numberOfPurchases; i++)
        {
            int productId = _random.Next(1, productCount + 1);
            int quantity = _random.Next(1, 4); 

            using (var cmd = new NpgsqlCommand("INSERT INTO Purchases (user_id, product_id, quantity) VALUES (@UserId, @ProductId, @Quantity);", conn))
            {
                cmd.Parameters.AddWithValue("@UserId", userId);
                cmd.Parameters.AddWithValue("@ProductId", productId);
                cmd.Parameters.AddWithValue("@Quantity", quantity);
                cmd.ExecuteNonQuery();
            }
        }
    }

    private void GenerateFollowsForUser(NpgsqlConnection conn, int userId, int userCount)
    {
        int numberOfFollows = _random.Next(0, 21); 
        var followedUsers = new HashSet<int>(); 

        while (followedUsers.Count < numberOfFollows)
        {
            int followedId = _random.Next(1, userCount + 1);
            if (followedId != userId && followedUsers.Add(followedId)) 
            {
                using (var cmd = new NpgsqlCommand("INSERT INTO Subscriptions (follower_id, followed_id) VALUES (@FollowerId, @FollowedId);", conn))
                {
                    cmd.Parameters.AddWithValue("@FollowerId", userId);
                    cmd.Parameters.AddWithValue("@FollowedId", followedId);
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }

    private int GetCount(NpgsqlConnection conn, string tableName)
    {
        using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {tableName};", conn))
        {
            return Convert.ToInt32(cmd.ExecuteScalar());
        }
    }
    
    public ProductBuyerFollowerCount GetProductBuyerAndFollowerCounts(int productId)
    {
        var query = @"
        SELECT
          p.product_id,
          p.name AS product_name,
          COUNT(DISTINCT pu.user_id) AS buyer_count,
          COUNT(DISTINCT s.follower_id) AS follower_buyer_count
        FROM
          purchases pu
        INNER JOIN products p ON pu.product_id = p.product_id
        LEFT JOIN Subscriptions s ON pu.user_id = s.followed_id
        LEFT JOIN purchases f ON s.follower_id = f.user_id AND f.product_id = pu.product_id
        WHERE
          p.product_id = @ProductId
        GROUP BY
          p.product_id;";

        try
        {
            using (var conn = new NpgsqlConnection(_connectionString))
            {
                conn.Open();
                var result = conn.QueryFirstOrDefault<ProductBuyerFollowerCount>(query, new { ProductId = productId });
                return result; // Retourne le résultat ou null si aucun résultat n'est trouvé
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
            return null; // Retourne null en cas d'erreur
        }
    }
    public List<UserPurchase> GetProductPurchasesByFollowerLevelsForProduct(int userId, int productId)
    {
        var query = @"
            WITH RECURSIVE follower_levels AS (
                SELECT follower_id, followed_id, 1 AS level
                FROM Subscriptions
                WHERE followed_id = @UserId
                UNION ALL
                SELECT s.follower_id, s.followed_id, fl.level + 1
                FROM Subscriptions s
                JOIN follower_levels fl ON s.followed_id = fl.follower_id
                WHERE fl.level < 4 -- Ajout de la condition pour s'arrêter à la profondeur 4
            ),
            purchases_by_level AS (
                SELECT fl.level, p.product_id, SUM(p.quantity) AS total_quantity
                FROM follower_levels fl
                JOIN Purchases p ON fl.follower_id = p.user_id
                WHERE p.product_id = @ProductId
                GROUP BY fl.level, p.product_id
            ),
            products_ordered AS (
                SELECT pbl.level, pr.name, pbl.total_quantity
                FROM purchases_by_level pbl
                JOIN Products pr ON pbl.product_id = pr.product_id
                ORDER BY pbl.level, pbl.total_quantity DESC
            )
            SELECT * FROM products_ordered;";

    
        try
        {
            using (var conn = new NpgsqlConnection(_connectionString))
            {
                conn.Open();
                var productPurchases = conn.Query<UserPurchase>(query, new { UserId = userId, ProductId = productId }).AsList();
                return productPurchases;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
            return new List<UserPurchase>();
        }
    }

    public List<UserPurchase> GetProductPurchasesByFollowerLevels(int userId)
    {
        var query = @"
        WITH RECURSIVE follower_levels AS (
            SELECT follower_id, followed_id, 1 AS level
            FROM Subscriptions
            WHERE followed_id = @UserId
            UNION ALL
            SELECT s.follower_id, s.followed_id, fl.level + 1
            FROM Subscriptions s
            JOIN follower_levels fl ON s.followed_id = fl.follower_id
            WHERE fl.level < 4 -- Limite la récursion à une profondeur de 4
        ),
        purchases_by_level AS (
            SELECT fl.level, p.product_id, SUM(p.quantity) AS total_quantity
            FROM follower_levels fl
            JOIN Purchases p ON fl.follower_id = p.user_id
            GROUP BY fl.level, p.product_id
        ),
        products_ordered AS (
            SELECT pbl.level, pr.name, pbl.total_quantity
            FROM purchases_by_level pbl
            JOIN Products pr ON pbl.product_id = pr.product_id
            ORDER BY pbl.level, pbl.total_quantity DESC
        )
        SELECT * FROM products_ordered;";

        try
        {
            using (var conn = new NpgsqlConnection(_connectionString))
            {
                conn.Open();
                var productPurchases = conn.Query<UserPurchase>(query, new { UserId = userId }).ToList();
                return productPurchases;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
            return new List<UserPurchase>(); 
        }
    }


}
