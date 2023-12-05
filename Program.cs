using Microsoft.Data.SqlClient;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

//builder.Services.AddEndpointsApiExplorer();
//builder.Services.AddSwaggerGen();

var app = builder.Build();

/*// For production scenarios, consider keeping Swagger configurations behind the environment check
// if (app.Environment.IsDevelopment())
// {
    app.UseSwagger();
    app.UseSwaggerUI();
// }*/

app.UseHttpsRedirection();

string connectionString = app.Configuration.GetConnectionString("AZURE_SQL_CONNECTIONSTRING")!;

app.MapGet("/usuario", () => {
    var rows = new List<string>();

    using var conn = new SqlConnection(connectionString);
    conn.Open();

    var command = new SqlCommand("SELECT * FROM usuario", conn);
    using SqlDataReader reader = command.ExecuteReader();

    if (reader.HasRows)
    {
        while (reader.Read())
        {
            rows.Add($"{reader.GetString(0)}, {reader.GetString(1)}");
        }
    }

    return rows;
})
.WithName("GetUsuario");


app.MapPost("/usuario", (Usuario usuario) => {
    using var conn = new SqlConnection(connectionString);
    conn.Open();

    var command = new SqlCommand(
        "INSERT INTO usuario (email, nome) VALUES (@email, @nome)",
        conn);

    command.Parameters.Clear();
    command.Parameters.AddWithValue("@email", usuario.email);
    command.Parameters.AddWithValue("@nome", usuario.nome);

    using SqlDataReader reader = command.ExecuteReader();
})
.WithName("CreateUsuario");

app.MapGet("/receita", () => {
    var rows = new List<string>();

    using var conn = new SqlConnection(connectionString);
    conn.Open();

    var command = new SqlCommand("SELECT u.nome AS nome_usuario, r.titulo AS titulo_receita, r.conteudo AS conteudo_receita FROM usuario u JOIN receitas r ON u.email = r.id_usuario", conn);
    using SqlDataReader reader = command.ExecuteReader();

    if (reader.HasRows)
    {
        while (reader.Read())
        {
            rows.Add($"{reader.GetString(0)}, {reader.GetString(1)}, {reader.GetString(2)}");
        }
    }

    return rows;
})
.WithName("GetReceita");

app.MapPost("/receita", (Receita receita) => {
    using var conn = new SqlConnection(connectionString);
    conn.Open();

    var command = new SqlCommand(
        "INSERT INTO receitas (id_usuario, titulo, conteudo) VALUES (@id_usuario, @titulo, @conteudo)",
        conn);

    command.Parameters.Clear();
    command.Parameters.AddWithValue("@id_usuario", receita.id_usuario);
    command.Parameters.AddWithValue("@titulo", receita.titulo);
    command.Parameters.AddWithValue("@conteudo", receita.conteudo);

    using SqlDataReader reader = command.ExecuteReader();

    var config = new ProducerConfig
        {
            BootstrapServers = "trabalho-receitas.servicebus.windows.net:9093",
            SecurityProtocol = SecurityProtocol.SaslSsl, // Protocolo de segurança para SASL/SSL
            SaslMechanism = SaslMechanism.Plain, // Mecanismo de autenticação (PLAIN)
            SaslUsername = "$ConnectionString", // Nome de usuário
            SaslPassword = "Endpoint=sb://trabalho-receitas.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lB3cAFoIuKjkKoyVLI48YQvqNj8n6U5oo+AEhLeClIg=",
            //SslCaLocation = "/etc/ssl/certs/ca-certificates.crt", // Caminho para o certificado CA (pode variar dependendo do sistema)
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            string topic = "app-receitas";

            var message = new Message<Null, string>
            {
                Value = receita.titulo+";"+receita.id_usuario+";Nova Receita;"+receita.conteudo,
            };

            var deliveryReport = producer.ProduceAsync(topic, message).Result;

            //Console.WriteLine($"Enviado para a partição: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
        }
})
.WithName("CreateReceita");

app.Run();

public class Usuario
{
    public required string nome { get; set; }
    public required string email { get; set; }
}

public class Receita
{
    public string id_usuario { get; set; }
    public string titulo { get; set; }
    public string conteudo { get; set; }

}