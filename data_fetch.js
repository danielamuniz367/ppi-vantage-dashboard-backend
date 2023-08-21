const cron = require("node-cron");
const { getClient } = require("./db");

const API_BASE_URL = "https://interview-app-ppi.vercel.app/api/agent";

async function insertDataIntoTable(client, insertQuery, data, valueMapper) {
  const insertPromises = data.map(async (dataItem) => {
    const values = valueMapper(dataItem);
    await client.query(insertQuery, values);
  });
  await Promise.all(insertPromises);
}

async function fetchAndInsertAgentData(client) {
  try {
    const apiResponse = await fetch(API_BASE_URL);
    const jsonData = await apiResponse.json();

    const insertQuery = `
      INSERT INTO public.agent (id, display_name)
      VALUES ($1, $2)
      ON CONFLICT (id) DO UPDATE
      SET display_name = EXCLUDED.display_name;
    `;

    const valueMapper = (dataItem) => [dataItem.id, dataItem.display_name];
    await insertDataIntoTable(client, insertQuery, jsonData, valueMapper);

    console.log("1. Agent data fetched and inserted successfully.");
  } catch (error) {
    console.error("Error:", error);
  }
}

async function fetchAndInsertDeviceData(client) {
  try {
    const agents = await client.query(
      `SELECT * FROM public.agent ORDER BY id ASC;`
    );
    const devicePromises = agents.rows.map(async (agent) => {
      try {
        const apiResponse = await fetch(`${API_BASE_URL}/${agent.id}`);
        const jsonData = await apiResponse.json();

        const insertQuery = `
          INSERT INTO public.device (id, agent_id, display_name)
          VALUES ($1, $2, $3)
          ON CONFLICT (id) DO UPDATE
          SET agent_id = EXCLUDED.agent_id,
              display_name = EXCLUDED.display_name;
        `;

        const valueMapper = (dataItem) => [
          dataItem.id,
          dataItem.agent_id,
          dataItem.display_name,
        ];
        await insertDataIntoTable(client, insertQuery, jsonData, valueMapper);
      } catch (error) {
        console.error(`Error processing agent ${agent.id}:`, error);
      }
    });

    await Promise.all(devicePromises);
    console.log("2. Device table updated successfully");
  } catch (error) {
    console.error("Error:", error);
  }
}

async function fetchAndInsertUptimeData(client) {
  try {
    const devices = await client.query(
      `SELECT * FROM public.device ORDER BY id ASC;`
    );
    const uptimePromises = devices.rows.map(async (device) => {
      try {
        const apiResponse = await fetch(
          `${API_BASE_URL}/${device.agent_id}/device/${device.id}/uptime`
        );
        const jsonData = await apiResponse.json();

        const insertQuery = `
          INSERT INTO public.device_uptime (id, device_id, uptime)
          VALUES ($1, $2, $3)
          ON CONFLICT (id) DO UPDATE
          SET device_id = EXCLUDED.device_id,
              uptime = EXCLUDED.uptime;
        `;

        const valueMapper = (dataItem) => [
          dataItem.id,
          dataItem.device_id,
          dataItem.uptime,
        ];

        await insertDataIntoTable(client, insertQuery, jsonData, valueMapper);
      } catch (error) {
        console.error(`Error processing device ${device.id}:`, error);
      }
    });

    await Promise.all(uptimePromises);
    console.log("3. Uptimes data updated successfully");
  } catch (error) {
    console.error("Error:", error);
  }
}

cron.schedule("* * * * *", async () => {
  const client = await getClient();

  try {
    await fetchAndInsertAgentData(client);
    await fetchAndInsertDeviceData(client);
    await fetchAndInsertUptimeData(client);
  } finally {
    // Release the client back to the pool regardless of success or failure
    client.release();
  }
});
