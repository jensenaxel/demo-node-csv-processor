const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const { createObjectCsvWriter } = require("csv-writer");

const {
  INBOUND_FILES_DIR,
  OUNTBOUND_FILES_DIR,
  FIELDS,
} = require("./constants");

// create function that will create a key that will let us easily compare if the row is a duplicate.
const getKeyName = (row) => {
  return `${row[FIELDS.USER_ID]}-${row[FIELDS.FIRST_NAME]}-${row[FIELDS.LAST_NAME]}-${FIELDS.INSURANCE_COMPANY}`;
};

// Function to process CSV file
const processCSV = (filePath) => {
  // Create a readable stream to read the CSV file
  const readStream = fs.createReadStream(filePath);

  // keep track of all results by a key to be able to only keep the highest version
  const maxVersionAllResults = {};
  const groupedResults = {};

  // Parse the CSV content
  readStream
    .pipe(csv())
    .on("data", (row) => {
      // Convert version to integer
      row[FIELDS.VERSION] = parseInt(row[FIELDS.VERSION]);

      const key = getKeyName(row);

      // if the key already exists, then compare the version number and only keep the one that is higher;
      if (maxVersionAllResults[key]) {
        if (row[FIELDS.VERSION] > maxVersionAllResults[key][FIELDS.VERSION]) {
          maxVersionAllResults[key] = row;
        }
      } else {
        maxVersionAllResults[key] = row;
      }

      // if it doesnt exists in the grouped results then create the inital array
      if (!groupedResults.hasOwnProperty(row[FIELDS.INSURANCE_COMPANY])) {
        groupedResults[row[FIELDS.INSURANCE_COMPANY]] = [];
      }

      // add row to the proper group
      groupedResults[row[FIELDS.INSURANCE_COMPANY]].push(row);
    })
    .on("end", () => {
      for (const company in groupedResults) {
        // get the highest version user from the map we already created that only keeps the highest version
        // and replace it in the array
        for (let i = 0; i < groupedResults[company].length; i++) {
          const row = groupedResults[company][i];
          const key = getKeyName(row);
          groupedResults[company][i] = maxVersionAllResults[key];
        }

        // this line will convert the array into a set then back again removing duplicates.
        groupedResults[company] = [...new Set(groupedResults[company])];
        groupedResults[company].sort(sortByLastNameFirstNameAscending);
        writeToFile(company, groupedResults[company]);
      }
      console.log("CSV file successfully processed.");
    });
};

const writeToFile = (company, records) => {
  // replaces spaces with dashes in the file name
  const outputFilePath = `${OUNTBOUND_FILES_DIR}/${company.split(" ").join("-").toLowerCase()}-enrollees.csv`;
  const csvWriter = createObjectCsvWriter({
    path: outputFilePath,
    header: [
      { id: FIELDS.USER_ID, title: FIELDS.USER_ID },
      { id: FIELDS.FIRST_NAME, title: FIELDS.FIRST_NAME },
      { id: FIELDS.LAST_NAME, title: FIELDS.LAST_NAME },
      { id: FIELDS.VERSION, title: FIELDS.VERSION },
      { id: FIELDS.INSURANCE_COMPANY, title: FIELDS.INSURANCE_COMPANY },
    ],
  });

  csvWriter
    .writeRecords(records)
    .then(() =>
      console.log(`The ${outputFilePath} file was written successfully`),
    );
};

// sort function
const sortByLastNameFirstNameAscending = (a, b) => {
  // Compare last names
  if (a[FIELDS.LAST_NAME] !== b[FIELDS.LAST_NAME]) {
    return a[FIELDS.LAST_NAME].localeCompare(b[FIELDS.LAST_NAME]);
  }
  // If last names are the same, compare first names
  return a[FIELDS.FIRST_NAME].localeCompare(b[FIELDS.FIRST_NAME]);
};

/*
NOTES:

This is an example usage loops over a defined directory and only grabs csv files.
You may end up making this a lamba or something that gets all the files from an s3 bucket and parse it the same way.
if using an s3 bucket loading the csv would look slightly different than using the "fs" library.

Depending on the amount of files you're dealing with you may want to have these files in a daily folder by date that gets
filled up with another process like this
files/inbound/02-27-2024/
then it would output into this folder
files/outbound/02-27-2024/
*/

// Example usage:
// Function to loop through directory and process each CSV file
const processDirectory = (directoryPath) => {
  // Read directory contents
  fs.readdir(directoryPath, (err, files) => {
    if (err) {
      console.error("Error reading directory:", err);
      return;
    }

    // Process each file
    files.forEach((file) => {
      // Check if the file is a CSV file
      if (path.extname(file).toLowerCase() === ".csv") {
        const filePath = path.join(directoryPath, file);
        processCSV(filePath);
      }
    });
  });
};

// Call the function to process the directory
processDirectory(INBOUND_FILES_DIR);
