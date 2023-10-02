import * as fs from "fs"; // Filesystem operations
import * as https from "https"; // HTTP requests
import * as zlib from "zlib"; // Data compression
import * as tar from "tar"; // TAR archive extraction
import * as csv from "fast-csv"; // CSV parsing
import knex, { Knex } from "knex"; // SQL query builder
import { DUMP_DOWNLOAD_URL, SQLITE_DB_PATH } from "./resources";

// Function to download a file from a given URL and save it to a specified path
/**
 *
 * @param url - URL of the file from Fiber AI
 * @param filePath
 * @returns
 */
async function downloadFile(url: string, filePath: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    // Create a write stream to save the downloaded file
    const fileStream = fs.createWriteStream(filePath);

    // Send an HTTP GET request to the provided URL
    https
      .get(url, (response) => {
        if (response.statusCode === 200) {
          // If the response status code is 200 (OK), pipe the response data to the file stream
          response.pipe(fileStream);
          // Listen for the 'finish' event to know when the file download is complete
          fileStream.on("finish", () => {
            fileStream.close(); // Close the file stream
            resolve(); // Resolve the promise to indicate successful download
          });
        } else {
          // If the response status code is not 200, reject the promise with an error
          reject(
            new Error(
              `Failed to download file. Status code: ${response.statusCode}`
            )
          );
        }
      })
      .on("error", (error) => {
        // Handle any errors that occur during the HTTP request
        reject(error);
      });
  });
}

// Function to determine the structure of a CSV file
/**
 *
 * @param csvPath - CSV Path of the customers and organizations
 * @returns - Returns the csv structure of both the file
 */
function getCsvStructure(csvPath: string): Promise<any> {
  return new Promise((resolve, reject) => {
    const csvStructure: any = {}; // Store CSV structure information

    // Create a readable stream to read the CSV file
    fs.createReadStream(csvPath)
      .pipe(csv.parse({ headers: true })) // Parse the CSV with headers
      .on("data", (row) => {
        // Analyze the first row to determine column names and data types
        if (!csvStructure.columns) {
          csvStructure.columns = Object.keys(row);
          csvStructure.types = {};

          // Initialize data types to null
          csvStructure.columns.map((column) => {
            csvStructure.types[column] = null;
          });
        }

        // Analyze subsequent rows to determine data types
        const updatedTypes = csvStructure.columns.reduce(
          (types, column) => {
            if (!types[column]) {
              const value = row[column];
              if (/^-?\d+$/.test(value)) {
                types[column] = "integer"; // Set data type to integer for numeric values
              } else if (!isNaN(Date.parse(value))) {
                types[column] = "datetime"; // Set data type to datetime for date values
              } else {
                types[column] = "string"; // Set data type to string for other values
              }
            }
            return types;
          },
          { ...csvStructure.types }
        );
        csvStructure.types = updatedTypes;
      })
      .on("end", () => {
        resolve(csvStructure); // Resolve the promise with the CSV structure
      })
      .on("error", (error) => {
        reject(error); // Reject the promise if an error occurs
      });
  });
}

// Function to create a table in the database from a CSV file
/**
 *
 * @param csvPath - Path of the CSV file
 * @param tableName - Table Name to be created
 */
async function createTableFromCsv(csvPath: string, tableName: string) {
  // Step 1: Parse the CSV file to determine its structure
  const csvStructure = await getCsvStructure(csvPath);

  // Step 2: Generate the knex schema dynamically based on the CSV structure
  const db = knex({
    client: "sqlite3",
    connection: {
      filename: "./out/database.sqlite", // Database file path
    },
    useNullAsDefault: true, // Use NULL as default for missing values
  });

  // Create the table with dynamic column types based on the CSV structure
  await db.schema.createTable(tableName, (table) => {
    csvStructure.columns.map((column) => {
      const dataType = csvStructure.types[column];
      // Determine the appropriate column type based on the data type
      if (dataType === "integer") {
        table.integer(column); // Integer column
      } else if (dataType === "datetime") {
        table.dateTime(column); // DateTime column
      } else {
        table.string(column); // String column
      }
    });
  });

  // Step 2: Create a data stream to read the CSV file
  const dataStream = fs
    .createReadStream(csvPath)
    .pipe(csv.parse({ headers: true }));

  // Step 3: Define a function to insert rows in batches
  const batchInsert = async (rows: any[]) => {
    try {
      await db(tableName).insert(rows);
    } catch (error) {
      console.error(`Error inserting data into ${tableName}:`, error);
    }
  };

  let rowCount = 0;
  let batch: any[] = [];

  // Step 4: Listen for 'data' events to read and insert data in batches
  dataStream.on("data", (row) => {
    rowCount++;
    batch.push(row);

    if (batch.length === 100) {
      batchInsert(batch);
      batch = [];
    }
  });

  // Step 5: Listen for 'end' event to insert any remaining rows
  dataStream.on("end", () => {
    if (batch.length > 0) {
      batchInsert(batch);
    }
    console.log(`Inserted ${rowCount} rows into ${tableName} table.`);
  });

  // Close the database connection
  // await db.destroy()
  //   .then(() => {
  //     console.log('Database connection closed.');
  //   })
  //   .catch((error) => {
  //     console.error('Error closing database connection:', error);
  //   });
}

// Function to extract and load data from the dump
async function extractAndLoadData(): Promise<void> {
  const tmpDir = ".";
  const outDir = "./out";
  const dumpFilePath = "./dump.tar.gz";

  try {
    // Step 1: Download the .tar.gz file
    console.log("Downloading the file...");
    await downloadFile(
      "https://fiber-challenges.s3.amazonaws.com/dump.tar.gz",
      dumpFilePath
    );
    console.log("Download completed.");

    // Step 2: Extract the TAR archive from the .tar.gz file
    await fs.promises.mkdir(tmpDir, { recursive: true });
    await fs.promises.mkdir(outDir, { recursive: true });

    console.log("Extracting the archive...");
    await new Promise<void>((resolve, reject) => {
      fs.createReadStream(dumpFilePath)
        .pipe(zlib.createGunzip()) // Decompress GZIP
        .pipe(tar.extract({ cwd: tmpDir, strip: 1 })) // Extract TAR
        .on("end", () => {
          console.log("Extraction completed.");
          resolve();
        })
        .on("error", (error) => {
          reject(error);
        });
    });

    // Step 3: Set up customers table in database.sqlite using knex
    await createTableFromCsv("customers.csv", "customers")
      .then(() => {
        console.log('Table "customers" created successfully');
      })
      .catch((error) => {
        console.error('Error creating table "customers":', error);
      });
    // Step 4: Set up organizations table in database.sqlite using knex
    await createTableFromCsv("organizations.csv", "organizations")
      .then(() => {
        console.log('Table "organizations" created successfully');
      })
      .catch((error) => {
        console.error('Error creating table "organizations":', error);
      });

    await console.log("Data processing completed.");
  } catch (error) {
    console.error("Error:", error);
  }
}

// Calling the helper method - extractAndLoadData
export async function processDataDump() {
  try {
    await extractAndLoadData();
  } catch (error) {
    console.error("Error:", error);
  }
}
