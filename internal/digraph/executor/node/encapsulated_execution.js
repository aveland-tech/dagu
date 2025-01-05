const { fork } = require('child_process');
const path = require('path');

// Configuration for encapsulation
const args = {
    scriptPath: process.argv[2],
    definition: JSON.parse(process.argv[3]),
    rawInputs: JSON.parse(process.argv[4])
};

// Print the arguments
process.argv.forEach((arg, index) => {
    console.log(`Argument ${index}: ${arg}`);
});

// Set NODE_PATH to include global node_modules
const globalNodeModules = require('child_process')
    .execSync('npm root -g')
    .toString().trim();
process.env.NODE_PATH = globalNodeModules;
require('module').Module._initPaths();

/**
 * Validate and filter the inputs.
 * @param {Object} rawInputs - Raw input object.
 * @param {Object} expectedInputs - Expected input object.
 * @returns {Object} validated inputs.
 */
function validateInputs(rawInputs, expectedInputs) {
    const validatedInputs = {};
    Object.keys(expectedInputs).forEach((key) => {
        const isRequired = expectedInputs[key].required || false;
        if (rawInputs[key] !== undefined) {
            validatedInputs[key] = rawInputs[key];
        } else if (isRequired) {
            throw new Error(`Missing required input: ${key}`);
        }
    });
    return validatedInputs;
}

/**
 * Execute the user-defined script with encapsulated input/output handling.
 * @param {Object} inputs - Inputs to pass to the user script.
 */
function executeScript(inputs) {
    console.log('Executing user script:', args.scriptPath);
    const userScript = fork(args.scriptPath, [], { silent: false });
    console.log('Sending Inputs:', inputs);
    // Send validated inputs to the user script
    userScript.send({ action: 'execute', inputs });

    // Handle messages from the user script
    userScript.on('message', (message) => {
        if (message.action === 'result') {
            console.log('User Script Output:', message.data);
        } else if (message.action === 'error') {
            console.error('User Script Error:', message.error);
        }
        userScript.kill();
    });

    userScript.on('exit', (code) => {
        if (code !== 0) {
            console.error('User script exited with an error.');
        }
    });
}

// Main execution
(async () => {
    try {
        const validatedInputs = validateInputs(args.rawInputs, args.definition.Inputs);
        if (Object.keys(validatedInputs).length === 0) {
            console.error('No valid inputs provided. Terminating execution.');
            process.exit(1);
        }

        executeScript(validatedInputs);
    } catch (error) {
        console.error('Failed to parse inputs:', error);
        process.exit(1);
    }
})();
