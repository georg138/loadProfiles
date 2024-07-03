var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

Database database=new();

app.MapGet("/", async () => await database.GetGridCostAsync());

app.Run();

