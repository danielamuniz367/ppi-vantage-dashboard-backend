const { Pool } = require("pg");

const clientPool = new Pool({
  user: "interviewexercisesadmin",
  host: "interview-exercises-ppi.postgres.database.azure.com",
  database: "daniela_muniz",
  password: "Qd9r9auGY2rF6MnEGw82",
  port: 5432,
  ssl: true,
});

// Function to get a client from the pool
const getClient = () => {
  return clientPool.connect();
};

// Function to test the database connection
const testDatabaseConnection = async () => {
  const client = await getClient();

  try {
    const result = await client.query("SELECT * FROM public.agent");
    console.log("Connected to the database:", result.rows[0]);
  } catch (error) {
    console.error("Error connecting to the database:", error);
  } finally {
    // Release the client back to the pool
    client.release();
  }
};

testDatabaseConnection();

// Export the getClient function
module.exports = {
  getClient,
};
