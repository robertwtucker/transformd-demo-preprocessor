/**
 * Copyright (c) 2023 Quadient Group AG
 * SPDX-License-Identifier: MIT
 */

import * as jp from 'jsonpath'

export function getDescription(): ScriptDescription {
  return {
    description:
      'Demo preprocessor script for integration with Transformd. Processes an input JSON file and stores data used by the Transformd Connector in a subsequent pipeline step.',
    icon: 'action',
    input: [
      {
        id: 'inputDataFile',
        displayName: 'Input Data File',
        description: 'JSON-formatted input data file to read from.',
        type: 'InputResource',
        required: true,
      },
      {
        id: 'outputSearchValuesFile',
        displayName: 'Output Search Values File',
        description:
          'Output file to store the calculated search value(s) for use in a subsequent Transformd Connector step.',
        type: 'OutputResource',
        required: true,
      },
      {
        id: 'sessionSearchPath',
        displayName: 'Form Session Search Value Path',
        description:
          'A JSONPath expression for the input data element(s) to use to calculate a unique form session identifier.',
        defaultValue:
          'concat("-", $.Clients[*].ClientID, $.Clients[*].ClaimID)',
        type: 'String',
        required: true,
      },
    ],
    output: [],
  }
}

export async function execute(context: Context): Promise<void> {
  // Delete output file (if it exists)
  //
  try {
    await context.getFile(context.parameters.outputSearchValuesFile as string).delete()
  } catch (err) {
    // Ignore error if file does not exist
  }

  // Read input data
  console.log(`Reading input file: ${context.parameters.inputDataFile}`)
  const inputData = await context.read(
    context.parameters.inputDataFile as string
  )

  // Process input data file using JSONPath expression provided via
  // 'sessionSearchPath' input param.
  //
  let inputJson = {}
  try {
    inputJson = JSON.parse(inputData)
  } catch (err) {
    throw new Error('Failed to parse input data as JSON.')
  }

  const searchValues = getSearchValues(
    inputJson,
    context.parameters.sessionSearchPath as string
  )

  // Write the calculated search values to the output file
  //
  const outputFile = context.parameters.outputSearchValuesFile as string
  console.log(`Writing search values to file: ${outputFile}`)
  await context.write(outputFile, searchValues.toString())

  console.log('Done.')
}

/**
* Retrieves the search (node) value(s) from a JSON object as specified by the
* JSONPath expression(s) provided. To support derived key fields, a synthetic
* 'concat()' function is made available. Usage:
*     concat(<delimiter>, <JSONPath expr 1>, <JSONPath expr 2, ...)
* @param {object} json JSON object to interrogate for values
* @param {string} searchPath JSONPath expression(s) for the search value
* @returns {string[]} An array of resolved search value(s)
*/
function getSearchValues(json: object, searchPath: string): string[] {
  const concatStartToken = 'concat('
  const concatEndToken = ')'
  let searchValues: string[] = []

  // Handle concat()'d' JSONPath expressions
  //
  if (searchPath.startsWith(concatStartToken)) {
    const concatArgs = searchPath.substring(
      concatStartToken.length,
      searchPath.endsWith(concatEndToken)
        ? searchPath.length - 1
        : searchPath.length
    )
    // Validate concat() arguments
    //
    const pathArgs = concatArgs.split(',')
    if (pathArgs && pathArgs.length < 3) {
      throw new Error('Invalid number or arguments to concat().')
    }
    const concatString = pathArgs!.shift()!.trim()
    // Concatenate the individually resolved JSONPath expressions
    //
    for (let i = 0; i < pathArgs.length; i++) {
      const pathArg = pathArgs[i].trim() ?? ''
      if (pathArg.startsWith('$')) {
        const nodeValues = getJsonPathNodeValues(json, pathArg)
        if (nodeValues.length > 0) {
          if (searchValues.length > 0) {
            searchValues = searchValues.map((element, index) =>
              element.concat(nodeValues[index])
            )
          } else {
            searchValues = nodeValues
          }
          // Add the delimiter if more search values to resolve
          //
          if (i < pathArgs.length - 1) {
            for (let j = 0; j < searchValues.length; j++) {
              searchValues[j] = searchValues[j].concat(concatString)
            }
          }
      } else {
        throw new Error(
          `No values found using the JSONPath expression '${pathArg}'`
        )
        }
      } else {
        throw new Error(
          `Invalid JSONPath argument to concat(): '${pathArg}' (missing root symbol).`
        )
      }
    }
  } else {
  // Single JSONPath expression to resolve
  //
  searchValues = getJsonPathNodeValues(json, searchPath)
  }

  console.log(`searchValues: ${searchValues}`)
  return searchValues
}

/**
* Helper function to parse the actual node values from the JSON object.
* @param {object} json JSON object
* @param {path} path JSONPath expression to the required node(s)
* @returns {string[]} A string array containing the node value(s)
*/
function getJsonPathNodeValues(json: object, path: string): string[] {
  try {
    const nodes = jp.nodes(json, path)
    return nodes.flatMap((node) => node.value)
  } catch (err) {
    console.log(`Error getting value (${path}) from JSON: ${err}`)
    return []
  }
}
