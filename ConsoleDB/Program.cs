// See https://aka.ms/new-console-template for more information

using System;
using ConsoleDB;

class Program
{
    

    static void Main(string[] args)
    {
        Console.Clear();
        Console.WriteLine("Bienvenue dans votre application de gestion de données !");
        Console.WriteLine("Veuillez choisir une base de données :");
        Console.WriteLine("1. Base de données NoSQL");
        Console.WriteLine("2. SGBDR");
        Console.WriteLine("3. Quitter");

        int choixBaseDeDonnees;
        if (!int.TryParse(Console.ReadLine(), out choixBaseDeDonnees))
        {
            Console.WriteLine("Veuillez entrer un numéro valide.");
            return;
        }

        switch (choixBaseDeDonnees)
        {
            case 1:
                UtiliserBaseDeDonneesNoSQL();
                break;
            case 2:
                UtiliserSGBDR();
                break;
            case 3:
                Console.WriteLine("Merci d'avoir utilisé notre application. À bientôt !");
                break;
            default:
                Console.WriteLine("Option non valide. Veuillez choisir une option valide.");
                break;
        }
    }

    static void UtiliserBaseDeDonneesNoSQL()
    {

    }

    static void UtiliserSGBDR()
    {
        Console.Clear();
        Console.WriteLine("Utilisation du SGBDR...");
        var connectionString = "Host=localhost;Port=5432;Database=postgres;Username=user;Password=password;";
        var dbConnector = new DatabaseConnector(connectionString);
        dbConnector.InitializeDatabaseTables();
        dbConnector.TestConnection();
        Console.WriteLine("1. Get Product Purchases By Follower Levels");
        Console.WriteLine("2. Get Product Buyer And Follower Counts");
        Console.WriteLine("3. Get Product Purchases By Follower Levels For Product");
        Console.WriteLine("4. Insert Users");
        
        int choix;
        int userid;
        int productid;
        var stopwatch = new System.Diagnostics.Stopwatch();
       
        List<UserPurchase> userPurchases;
        
        
        if (!int.TryParse(Console.ReadLine(), out choix))
        {
            Console.WriteLine("Veuillez entrer un numéro valide.");
            return;
        }
        switch (choix)
        {
            case 1:
                
                Console.WriteLine("Saisisez l'id utilisateur(int) : ");
                if (Int32.TryParse(Console.ReadLine(), out userid))
                {
                    stopwatch.Start();
                    userPurchases=dbConnector.GetProductPurchasesByFollowerLevels(userid);
                    if (userPurchases.Count == 0)
                    {
                        Console.WriteLine("Aucun achat trouvé pour cet utilisateur ou ses suiveurs.");
                    }
                    else
                    {
                        foreach (var purchase in userPurchases)
                        {
                            Console.WriteLine($"Niveau: {purchase.level}, Produit: {purchase.name}, Quantité: {purchase.total_quantity}");
                        }
                    }
                    stopwatch.Stop(); 
                    Console.WriteLine($"GetProductBuyerAndFollowerCounts {stopwatch.Elapsed.TotalSeconds}");
                }
                else
                {
                    Console.WriteLine("L'ID utilisateur fourni n'est pas valide.");
                }
                
                break;
            case 2:
                Console.WriteLine("Saisisez l'id utilisateur(int) : ");
                if (Int32.TryParse(Console.ReadLine(), out userid))
                {
                    Console.WriteLine("Saisisez l'id produit(int) : ");
                    if (Int32.TryParse(Console.ReadLine(), out productid))
                    {
                        stopwatch.Start();
                        userPurchases=dbConnector.GetProductPurchasesByFollowerLevelsForProduct(userid, productid);
                        if (userPurchases.Count == 0)
                        {
                            Console.WriteLine("Aucun achat trouvé pour cet utilisateur ou ses suiveurs.");
                        }
                        else
                        {
                            foreach (var purchase in userPurchases)
                            {
                                Console.WriteLine($"Niveau: {purchase.level}, Produit: {purchase.name}, Quantité: {purchase.total_quantity}");
                            }
                        }
                        stopwatch.Stop(); 
                        Console.WriteLine($"GetProductBuyerAndFollowerCounts {stopwatch.Elapsed.TotalSeconds}");
                        
                    }
                    else
                    {
                        Console.WriteLine("L'ID produit fourni n'est pas valide.");
                    }
                }
                else
                {
                    Console.WriteLine("L'ID utilisateur fourni n'est pas valide.");
                }
                break;
            case 3:
                Console.WriteLine("Saisissez l'id du produit (int) : ");
                if (Int32.TryParse(Console.ReadLine(), out int productId))
                {   
                    stopwatch.Start();
                    var productBuyerFollowerCount = dbConnector.GetProductBuyerAndFollowerCounts(productId);
                    if (productBuyerFollowerCount != null)
                    {
                        Console.WriteLine($"Produit: {productBuyerFollowerCount.product_name}, Acheteurs: {productBuyerFollowerCount.buyer_count}, Suiveurs ayant acheté: {productBuyerFollowerCount.follower_buyer_count}");
                    }
                    else
                    {
                        Console.WriteLine("Aucune donnée trouvée pour ce produit.");
                    }
                    stopwatch.Stop(); 
                    Console.WriteLine($"GetProductBuyerAndFollowerCounts {stopwatch.Elapsed.TotalSeconds}");
                }
                else
                {
                    Console.WriteLine("L'ID produit fourni n'est pas valide.");
                }
                break;
            
            case 4:

                int n_products = 10000;
                stopwatch.Start(); 
                Console.WriteLine($"insertion de {n_products} produits ...");
                dbConnector.InsertProducts(n_products);
                stopwatch.Stop(); 
                Console.WriteLine($"Insertion de {n_products} produits terminée en {stopwatch.Elapsed.TotalSeconds} secondes.");

                Console.WriteLine($"Combien d'utilisateurs voulez-vous insérer (int) ?");
                if (Int32.TryParse(Console.ReadLine(), out int n_user))
                {
                    Console.WriteLine($"Insertion de {n_user} utilisateurs ...");
                    stopwatch.Restart(); 
                    var (firstUserId, lastUserId) = dbConnector.GenerateUsers(n_user);
                    stopwatch.Stop(); 
                    Console.WriteLine($"Insertion des utilisateurs terminée en {stopwatch.Elapsed.TotalSeconds} secondes.");
        
                    Console.WriteLine($"Insertion des achats et des suivis ...");
                    stopwatch.Restart(); 
                    dbConnector.GenerateDataForAllUsers(firstUserId, lastUserId);
                    stopwatch.Stop();
                    Console.WriteLine($"Insertion des achats et des suivis terminée en {stopwatch.Elapsed.TotalSeconds} secondes.");
                }
                else
                {
                    Console.WriteLine("Le nombre d'utilisateurs saisi est invalide.");
                }
                break;
            default:
                Console.WriteLine("Option non valide. Veuillez choisir une option valide.");
                break;
        }
        
    }

    
} 