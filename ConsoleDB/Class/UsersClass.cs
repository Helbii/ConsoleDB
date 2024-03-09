namespace ConsoleDB;

public class UsersClass
{
    public int UserId { get; set; } 
    public string Name { get; set; }
    public string Surname { get; set; } 
    public DateTime CreatedAt { get; set; } 


    public UsersClass() { }

     
    public UsersClass(string name, string surname)
    {
        Name = name;
        Surname = surname;
        CreatedAt = DateTime.UtcNow; 
    }
}