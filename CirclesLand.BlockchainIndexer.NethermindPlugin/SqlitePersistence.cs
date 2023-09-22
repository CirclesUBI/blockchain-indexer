using Microsoft.Data.Sqlite;
using Nethermind.Core;

namespace Circles.Index;

public class SqlitePersistence : IDisposable, IAsyncDisposable
{
    private readonly SqliteConnection _connection;

    private const int TransactionLimit = 10000; // Number of inserts before committing a transaction
    private SqliteTransaction? _transaction;
    private int _transactionCounter;

    private SqliteCommand? _addVisitedBlockInsertCmd;
    private SqliteCommand? _addCirclesSignupInsertCmd;
    private SqliteCommand? _addCirclesTrustInsertCmd;
    private SqliteCommand? _addCirclesHubTransferInsertCmd;
    private SqliteCommand? _addCirclesTransferInsertCmd;
    private SqliteCommand? _addIrrelevantBlockCmd;

    private HashSet<Address> _circlesTokens = new();

    public SqlitePersistence(string dbPath)
    {
        _connection = new SqliteConnection($"Data Source={dbPath}");
        _connection.Open();
    }

    public void Initialize()
    {
        using SqliteCommand createTableCmd = _connection.CreateCommand();
        createTableCmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS block_relevant (
                block_number INTEGER PRIMARY KEY
            );
        ";
        createTableCmd.ExecuteNonQuery();

        using SqliteCommand createBlocksWithoutCirclesTxsTableCmd = _connection.CreateCommand();
        createBlocksWithoutCirclesTxsTableCmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS block_irrelevant (
                block_number INTEGER PRIMARY KEY
            );
        ";
        createBlocksWithoutCirclesTxsTableCmd.ExecuteNonQuery();

        using SqliteCommand createCirclesSignupTableCmd = _connection.CreateCommand();
        createCirclesSignupTableCmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS circles_signup (
                block_number INTEGER,
                transaction_hash TEXT,
                circles_address TEXT PRIMARY KEY,
                token_address TEXT NULL
            );
        ";
        createCirclesSignupTableCmd.ExecuteNonQuery();

        using SqliteCommand createCirclesTrustTableCmd = _connection.CreateCommand();
        createCirclesTrustTableCmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS circles_trust (
                block_number INTEGER,
                transaction_hash TEXT,
                user_address TEXT,
                can_send_to_address TEXT,
                ""limit"" INTEGER
            );
        ";
        createCirclesTrustTableCmd.ExecuteNonQuery();

        using SqliteCommand createCirclesHubTransferTableCmd = _connection.CreateCommand();
        createCirclesHubTransferTableCmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS circles_hub_transfer (
                block_number INTEGER,
                transaction_hash TEXT,
                from_address TEXT,
                to_address TEXT,
                amount INTEGER
            );
        ";
        createCirclesHubTransferTableCmd.ExecuteNonQuery();

        using SqliteCommand createCirclesTransferTableCmd = _connection.CreateCommand();
        createCirclesTransferTableCmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS circles_transfer (
                block_number INTEGER,
                transaction_hash TEXT,
                token_address TEXT,
                from_address TEXT,
                to_address TEXT,
                amount INTEGER
            );
        ";
        createCirclesTransferTableCmd.ExecuteNonQuery();

        // Warm up the caches (read all circles tokens)
        using SqliteCommand selectCirclesTokensCmd = _connection.CreateCommand();
        selectCirclesTokensCmd.CommandText = @"
            SELECT token_address
            FROM circles_signup
            WHERE token_address IS NOT NULL;
        ";
        using SqliteDataReader reader = selectCirclesTokensCmd.ExecuteReader();
        while (reader.Read())
        {
            string tokenAddress = reader.GetString(0);
            _circlesTokens.Add(new Address(tokenAddress));
        }

        reader.Close();
    }

    public bool IsCirclesToken(Address address)
    {
        return _circlesTokens.Contains(address);
    }

    public void AddIrrelevantBlock(long blockNumber)
    {
        PrepareAddIrrelevantBlockCommand();

        if (_transactionCounter == 0)
        {
            BeginTransaction();
        }

        _addIrrelevantBlockCmd.Transaction = _transaction;
        _addIrrelevantBlockCmd.Parameters.Clear(); // Clear previous parameters
        _addIrrelevantBlockCmd.Parameters.AddWithValue("@blockNumber", blockNumber);
        _addIrrelevantBlockCmd.ExecuteNonQuery();

        _transactionCounter++;

        if (_transactionCounter >= TransactionLimit)
        {
            CommitTransaction();
        }
    }

    public void AddVisitedBlock(long blockNumber)
    {
        PrepareAddVisitedBlockInsertCommand();

        if (_transactionCounter == 0)
        {
            BeginTransaction();
        }

        _addVisitedBlockInsertCmd.Transaction = _transaction;
        _addVisitedBlockInsertCmd.Parameters.Clear(); // Clear previous parameters
        _addVisitedBlockInsertCmd.Parameters.AddWithValue("@blockNumber", blockNumber);
        _addVisitedBlockInsertCmd.ExecuteNonQuery();

        _transactionCounter++;

        if (_transactionCounter >= TransactionLimit)
        {
            CommitTransaction();
        }
    }

    public void AddCirclesSignup(long blockNumber, string transactionHash, string circlesAddress, string? tokenAddress)
    {
        PrepareAddCirclesSignupInsertCommand();

        if (tokenAddress != null)
        {
            _circlesTokens.Add(new Address(tokenAddress));
        }

        if (_transactionCounter == 0)
        {
            BeginTransaction();
        }

        _addCirclesSignupInsertCmd.Transaction = _transaction;
        _addCirclesSignupInsertCmd.Parameters.Clear(); // Clear previous parameters
        _addCirclesSignupInsertCmd.Parameters.AddWithValue("@blockNumber", blockNumber);
        _addCirclesSignupInsertCmd.Parameters.AddWithValue("@transactionHash", transactionHash);
        _addCirclesSignupInsertCmd.Parameters.AddWithValue("@circlesAddress", circlesAddress);
        _addCirclesSignupInsertCmd.Parameters.AddWithValue("@tokenAddress", (object?)tokenAddress ?? DBNull.Value);
        _addCirclesSignupInsertCmd.ExecuteNonQuery();

        _transactionCounter++;

        if (_transactionCounter >= TransactionLimit)
        {
            CommitTransaction();
        }
    }

    public void AddCirclesTrust(long blockNumber, string toString, string userAddress, string canSendToAddress, int limit)
    {
        PrepareAddCirclesTrustInsertCommand();

        if (_transactionCounter == 0)
        {
            BeginTransaction();
        }

        _addCirclesTrustInsertCmd.Transaction = _transaction;
        _addCirclesTrustInsertCmd.Parameters.Clear(); // Clear previous parameters
        _addCirclesTrustInsertCmd.Parameters.AddWithValue("@blockNumber", blockNumber);
        _addCirclesTrustInsertCmd.Parameters.AddWithValue("@transactionHash", toString);
        _addCirclesTrustInsertCmd.Parameters.AddWithValue("@userAddress", userAddress);
        _addCirclesTrustInsertCmd.Parameters.AddWithValue("@canSendToAddress", canSendToAddress);
        _addCirclesTrustInsertCmd.Parameters.AddWithValue("@limit", limit);
        _addCirclesTrustInsertCmd.ExecuteNonQuery();

        _transactionCounter++;

        if (_transactionCounter >= TransactionLimit)
        {
            CommitTransaction();
        }
    }

    public void AddCirclesHubTransfer(long blockNumber, string toString, string fromAddress, string toAddress, string amount)
    {
        PrepareAddCirclesHubTransferInsertCommand();

        if (_transactionCounter == 0)
        {
            BeginTransaction();
        }

        _addCirclesHubTransferInsertCmd.Transaction = _transaction;
        _addCirclesHubTransferInsertCmd.Parameters.Clear(); // Clear previous parameters
        _addCirclesHubTransferInsertCmd.Parameters.AddWithValue("@blockNumber", blockNumber);
        _addCirclesHubTransferInsertCmd.Parameters.AddWithValue("@transactionHash", toString);
        _addCirclesHubTransferInsertCmd.Parameters.AddWithValue("@fromAddress", fromAddress);
        _addCirclesHubTransferInsertCmd.Parameters.AddWithValue("@toAddress", toAddress);
        _addCirclesHubTransferInsertCmd.Parameters.AddWithValue("@amount", amount);
        _addCirclesHubTransferInsertCmd.ExecuteNonQuery();

        _transactionCounter++;

        if (_transactionCounter >= TransactionLimit)
        {
            CommitTransaction();
        }
    }

    public void AddCirclesTransfer(long blockNumber, string toString, string tokenAddress, string from, string to,
        string value)
    {
        PrepareAddCirclesTransferInsertCommand();

        if (_transactionCounter == 0)
        {
            BeginTransaction();
        }

        _addCirclesTransferInsertCmd.Transaction = _transaction;
        _addCirclesTransferInsertCmd.Parameters.Clear(); // Clear previous parameters
        _addCirclesTransferInsertCmd.Parameters.AddWithValue("@blockNumber", blockNumber);
        _addCirclesTransferInsertCmd.Parameters.AddWithValue("@transactionHash", toString);
        _addCirclesTransferInsertCmd.Parameters.AddWithValue("@tokenAddress", tokenAddress);
        _addCirclesTransferInsertCmd.Parameters.AddWithValue("@fromAddress", from);
        _addCirclesTransferInsertCmd.Parameters.AddWithValue("@toAddress", to);
        _addCirclesTransferInsertCmd.Parameters.AddWithValue("@amount", value);
        _addCirclesTransferInsertCmd.ExecuteNonQuery();

        _transactionCounter++;

        if (_transactionCounter >= TransactionLimit)
        {
            CommitTransaction();
        }
    }

    public long GetLastRelevantBlock()
    {
        SqliteCommand selectCmd = _connection.CreateCommand();
        selectCmd.CommandText = @"
            SELECT block_number
            FROM block_relevant
            ORDER BY block_number DESC
            LIMIT 1;
        ";

        object? result = selectCmd.ExecuteScalar();
        if (result is null)
        {
            return 0;
        }

        return (long)result;
    }

    public void Dispose()
    {
        CommitTransaction(); // Ensure any remaining transactions are committed before disposing.
        _connection.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        CommitTransaction(); // Ensure any remaining transactions are committed before disposing.
        await _connection.DisposeAsync();
    }

    private void PrepareAddIrrelevantBlockCommand()
    {
        if (_addIrrelevantBlockCmd != null) return;

        _addIrrelevantBlockCmd = _connection.CreateCommand();
        _addIrrelevantBlockCmd.CommandText = @"
            INSERT OR IGNORE INTO block_irrelevant (block_number)
            VALUES (@blockNumber);
        ";
    }

    private void PrepareAddVisitedBlockInsertCommand()
    {
        if (_addVisitedBlockInsertCmd != null) return;

        _addVisitedBlockInsertCmd = _connection.CreateCommand();
        _addVisitedBlockInsertCmd.CommandText = @"
            INSERT INTO block_relevant (block_number)
            VALUES (@blockNumber);
        ";
    }

    private void PrepareAddCirclesSignupInsertCommand()
    {
        if (_addCirclesSignupInsertCmd != null) return;

        _addCirclesSignupInsertCmd = _connection.CreateCommand();
        _addCirclesSignupInsertCmd.CommandText = @"
            INSERT INTO circles_signup (block_number, transaction_hash, circles_address, token_address)
            VALUES (@blockNumber, @transactionHash, @circlesAddress, @tokenAddress);
        ";
    }

    private void PrepareAddCirclesTrustInsertCommand()
    {
        if (_addCirclesTrustInsertCmd != null) return;

        _addCirclesTrustInsertCmd = _connection.CreateCommand();
        _addCirclesTrustInsertCmd.CommandText = @"
            INSERT INTO circles_trust (block_number, transaction_hash, user_address, can_send_to_address, ""limit"")
            VALUES (@blockNumber, @transactionHash, @userAddress, @canSendToAddress, @limit);
        ";
    }

    private void PrepareAddCirclesHubTransferInsertCommand()
    {
        if (_addCirclesHubTransferInsertCmd != null) return;

        _addCirclesHubTransferInsertCmd = _connection.CreateCommand();
        _addCirclesHubTransferInsertCmd.CommandText = @"
            INSERT INTO circles_hub_transfer (block_number, transaction_hash, from_address, to_address, amount)
            VALUES (@blockNumber, @transactionHash, @fromAddress, @toAddress, @amount);
        ";
    }

    private void PrepareAddCirclesTransferInsertCommand()
    {
        if (_addCirclesTransferInsertCmd != null) return;

        _addCirclesTransferInsertCmd = _connection.CreateCommand();
        _addCirclesTransferInsertCmd.CommandText = @"
            INSERT INTO circles_transfer (block_number, transaction_hash, token_address, from_address, to_address, amount)
            VALUES (@blockNumber, @transactionHash, @tokenAddress, @fromAddress, @toAddress, @amount);
        ";
    }

    private void BeginTransaction()
    {
        if (_transaction != null) return;
        _transaction = _connection.BeginTransaction();
    }

    private void CommitTransaction()
    {
        _transaction?.Commit();
        _transaction = null;
        _transactionCounter = 0;
    }
}
