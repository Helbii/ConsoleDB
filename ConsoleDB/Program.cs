// See https://aka.ms/new-console-template for more information

using System;
using ConsoleDB;
using System.Diagnostics;
using System.Threading.Tasks;
using Neo4j.Driver;

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
        var uri = "bolt://localhost:7687";
        var username = "neo4j";
        var password = "luca";

        using var driver = GraphDatabase.Driver(uri, AuthTokens.Basic(username, password));
        await using var session = driver.AsyncSession(o => o.WithDatabase("neo4j"));

        Console.Clear();
        Console.WriteLine("Veuillez choisir une requete :");
        Console.WriteLine("1. Injecter des users");
        Console.WriteLine("2. Injecter des produits");
        Console.WriteLine("3. Quitter");
        
        int choix;
        if (!int.TryParse(Console.ReadLine(), out choix))
        {
            Console.WriteLine("Veuillez entrer un numéro valide.");
            return;
        }

        switch (choix)
        {
            case 1:
                Console.Clear();
                Console.WriteLine("Combien de users ?");
                int nombreUser = int.Parse(Console.ReadLine());
                await InjecterUsersDansBaseDeDonnees(session, nombreUser);
                break;
            case 2:
                Console.Clear();
                Console.WriteLine("Combien de produits ?");
                int nombreProduits = int.Parse(Console.ReadLine());
                await CreerProduits(session, nombreProduits);
                break;
            case 3:
                
                break;
            default:
                throw new InvalidOperationException("Choix invalide.");
        }

        Console.WriteLine($"Fini");
    }

    static async Task InjecterUsersDansBaseDeDonnees(IAsyncSession session, int nombreUtilisateur)
    {
        Console.WriteLine($"Vous avez choisi d'injecter {nombreUtilisateur} utilisateurs.");
    
        Stopwatch sw = new Stopwatch();
        sw.Start();

        var query = @"
        CREATE (n:User {id: $id, nom: $nom, prenom: $prenom})
        RETURN n
        ";

        var utilisateursCrees = new List<string>();
    
        for (int i = 0; i < nombreUtilisateur; i++)
        {
            var idUtilisateur = Guid.NewGuid().ToString(); 
            var nomUtilisateur = GenerateData.GenererNomPrenomAleatoire();
            var prenomUtilisateur = GenerateData.GenererNomPrenomAleatoire();
            var parameters = new { id = idUtilisateur, nom = nomUtilisateur, prenom = prenomUtilisateur };
        
            await session.WriteTransactionAsync(async transaction =>
            {
                var cursor = await transaction.RunAsync(query, parameters);
                var result = await cursor.SingleAsync();
                var createdNode = result["n"].As<INode>();
                //Console.WriteLine($"Utilisateur créé avec l'ID: {createdNode["id"].As<string>()}");
                utilisateursCrees.Add(createdNode["id"].As<string>());
            });
        }

        //Console.WriteLine($"Utilisateurs créés : {string.Join(", ", utilisateursCrees)}");
        await AjouterAbonnements(session, utilisateursCrees);
        await AjouterAchats(session, utilisateursCrees);

        sw.Stop();
        Console.WriteLine($"Temps d'exécution : {sw.ElapsedMilliseconds} ms");
    }

    static async Task AjouterAbonnements(IAsyncSession session, List<string> idsUtilisateurs)
    {
        var query = @"
        MATCH (u1:User {id: $id1}), (u2:User {id: $id2})
        CREATE (u1)-[:ABONNEMENT]->(u2)
        ";

        foreach (var idUtilisateur in idsUtilisateurs)
        {
            var nombreAbonnements = new Random().Next(0, 21); 

            for (int i = 0; i < nombreAbonnements; i++)
            {
                var utilisateurAbonnement = idsUtilisateurs[new Random().Next(idsUtilisateurs.Count)]; // Choix aléatoire d'un utilisateur existant comme abonnement
                if (utilisateurAbonnement != idUtilisateur) // S'assurer que l'utilisateur ne s'abonne pas à lui-même
                {
                    await session.WriteTransactionAsync(async transaction =>
                    {
                        await transaction.RunAsync(query, new { id1 = idUtilisateur, id2 = utilisateurAbonnement });
                    });

                    //Console.WriteLine($"Utilisateur avec l'ID: {idUtilisateur} s'est abonné à l'utilisateur avec l'ID: {utilisateurAbonnement}");
                }
            }
        }
    }
    
    static async Task CreerProduits(IAsyncSession session, int nombreProduits)
    {
        Console.WriteLine($"Vous avez choisi de créer {nombreProduits} produits.");

        Stopwatch sw = new Stopwatch();
        sw.Start();

        var query = @"
            CREATE (p:Produit {id: $id, nom: $nom, prix: $prix})
            RETURN p
        ";

        var produitsCrees = new List<string>();

        for (int i = 0; i < nombreProduits; i++)
        {
            var idProduit = Guid.NewGuid().ToString();
            var nomProduit = GenerateData.GenererNomProduitAleatoire();
            var prixProduit = GenerateData.GenererPrixAleatoire();
            var parameters = new { id = idProduit, nom = nomProduit, prix = prixProduit };

            await session.WriteTransactionAsync(async transaction =>
            {
                var cursor = await transaction.RunAsync(query, parameters);
                var result = await cursor.SingleAsync();
                var createdNode = result["p"].As<INode>();
                //Console.WriteLine($"Produit créé avec le nom: {createdNode["nom"].As<string>()} et le prix: {createdNode["prix"].As<double>()}");
                produitsCrees.Add(createdNode["nom"].As<string>());
            });
        }

        sw.Stop();
        Console.WriteLine($"Temps d'exécution : {sw.ElapsedMilliseconds} ms");

        //Console.WriteLine($"Produits créés : {string.Join(", ", produitsCrees)}");
    }
    
    static async Task AjouterAchats(IAsyncSession session, List<string> idsUtilisateurs)
    {
        var query = @"
        MATCH (u:User {id: $userId}), (p:Produit {id: $produitId})
        CREATE (u)-[:ACHAT]->(p)
        ";

        var produits = await session.ReadTransactionAsync(async transaction =>
        {
            var result = await transaction.RunAsync("MATCH (p:Produit) RETURN p.id AS produitId");
            return await result.ToListAsync();
        });

        var produitIds = produits.Select(p => p["produitId"].As<string>()).ToList();
        
        foreach (var idUtilisateur in idsUtilisateurs)
        {
            var nombreAchats = new Random().Next(0, 6);
            
            for (int i = 0; i < nombreAchats; i++)
            {
                var produitId = produitIds[new Random().Next(produitIds.Count)];

                await session.WriteTransactionAsync(async transaction =>
                {
                    await transaction.RunAsync(query, new { userId = idUtilisateur, produitId });
                });

                //Console.WriteLine($"L'utilisateur avec l'ID: {idUtilisateur} a acheté le produit avec l'ID: {produitId}");
            }

        }
    }
// -----------------------------------------------------------------------------------------------

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