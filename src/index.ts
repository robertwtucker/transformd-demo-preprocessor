/**
 * Copyright (c) 2023 Quadient Group AG
 * SPDX-License-Identifier: MIT
 */

import {
  JsonEvent,
  JsonEventType,
  JsonMaterializingParser,
  StringReadableStream,
} from '@quadient/evolve-data-transformations'

export function getDescription(): ScriptDescription {
  return {
    description:
      'Demo preprocessor script for integration with Transformd. Processes an input JSON file and stores data used in a subsequent pipeline step by the Transformd Connector.',
    icon: 'action',
    input: [
      {
        id: 'inputDataFile',
        displayName: 'Input data file',
        description: 'Data file to read input from (JSON format).',
        type: 'InputResource',
        required: true,
      },
      {
        id: 'outputSearchValuesFile',
        displayName: 'Output search values file',
        description:
          'Output file to store the calculated search value(s) in for use in a subsequent pipeline step.',
        type: 'OutputResource',
        required: true,
      },
      {
        id: 'sessionSearchPath',
        displayName: 'Form session search value path',
        description:
          'A JSONPath expression for the input data element(s) to use to calculate a unique form session identifier.',
        defaultValue: 'concat(-, $.Clients[*].ClientID, $.Clients[*].ClaimID)',
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
    await context
      .getFile(context.parameters.outputSearchValuesFile as string)
      .delete()
  } catch (err) {
    // Ignore error (i.e. file does not exist)
  }

  // Parse the JSONPath expression(s) provided via the 'sessionSearchPath'
  // input param to get the unique value(s) that will be used as an
  // identifier in a later step.
  //
  const sessionSearchPath = context.parameters.sessionSearchPath as string
  const pathExpressions = getSearchPathExpressions(sessionSearchPath)
  const concatDelimiter =
    pathExpressions.length > 1
      ? getConcatenatedDelimiter(sessionSearchPath)
      : ''

  let pathValues: string[] = [],
    fieldNames: string[] = []
  for (let i = 0; i < pathExpressions.length; i++) {
    const path = pathExpressions[i].split('.')
    fieldNames.push(path?.pop() ?? '')
    if (fieldNames[i].length === 0) {
      throw new Error(`Invalid JSONPath expression '${pathExpressions[i]}'.`)
    }
    pathValues.push(path!.join('.'))
  }

  let searchValues: string[] = []
  const parserCallback = async function (event: JsonEvent) {
    let parserValue = ''
    if (event.type === JsonEventType.ANY_VALUE) {
      const data = event.data
      for (let i = 0; i < fieldNames.length; i++) {
        parserValue =
          i < fieldNames.length - 1
            ? parserValue.concat(data[fieldNames[i]].concat(concatDelimiter))
            : parserValue.concat(data[fieldNames[i]])
      }
      searchValues.push(parserValue)
    }
  }

  // Read the input data
  //
  console.log(`Reading input file: ${context.parameters.inputDataFile}`)
  const inputData = await context.read(
    context.parameters.inputDataFile as string
  )

  // Parse the input data to resolve the search value JSONPath expression(s) to
  // the elements' actual data values.
  //
  const materializedPaths = [...new Set(pathValues)]
  const parser = new JsonMaterializingParser(parserCallback, {
    materializedPaths,
  })
  await parser.parse(inputData)
  await parser.flush()

  // Write the resolved search values to the output file in JSON format
  //
  const input = new StringReadableStream(
    '{"values":' + JSON.stringify(searchValues) + '}'
  )
  const outputFile = context.parameters.outputSearchValuesFile as string
  console.log(
    `Writing search values (${searchValues.length}) to file: ${outputFile}`
  )
  const outputStream = await context.openWriteText(outputFile)
  input.pipeTo(outputStream)

  console.log('Done.')
}

/**
 * Retrieves the search value(s) from a JSON object as specified by the
 * JSONPath expression(s) provided. To support derived key fields, a synthetic
 * 'concat()' function is made available. Usage:
 *     concat(<delimiter>, <JSONPath expr 1>, <JSONPath expr 2, ...)
 * @param {string} path JSONPath expression(s) for the search value
 * @returns {string[]} An array of resolved search path(s)
 */
function getSearchPathExpressions(path: string): string[] {
  let pathValues: string[] = []

  // Handle concat()'d' JSONPath expressions
  //
  if (isConcatenated(path)) {
    const pathArgs = getConcatenatedPathArgs(path)
    pathArgs!.shift() // Remove the concatDelimiter
    // Validate the individual JSONPath expressions
    //
    for (let i = 0; i < pathArgs.length; i++) {
      const pathArg = pathArgs[i].trim() ?? ''
      pathValues.push(hasRoot(pathArg) ? pathArg.substring(1) : pathArg)
    }
  } else {
    // Single JSONPath expression to validate
    //
    pathValues = Array.from(hasRoot(path) ? path.substring(1) : path)
  }

  return pathValues
}

/**
 * Helper function to determine if a JSONPath expression is concatenated.
 * @param {string} path JSONPath expression to the requested node(s)
 * @returns boolean value indicating whether the JSONPath expression starts with the concat keyword
 */
function isConcatenated(path: string): boolean {
  return path.startsWith('concat(')
}

/**
 * Helper function to parse and validate the arguments to the synthetic concat() function.
 * @param {string} path concatenated JSONPath expressions
 * @returns array of arguments to the synthetic concat() function
 */
function getConcatenatedPathArgs(path: string): string[] {
  const concatArgs = path.substring(
    'concat('.length,
    path.endsWith(')') ? path.length - 1 : path.length
  )
  // Validate concat() arguments
  //
  const pathArgs = concatArgs.split(',')
  if (pathArgs && pathArgs.length < 3) {
    throw new Error('Invalid number or arguments to concat().')
  }

  return pathArgs
}

/**
 * Helper function to parse out the concatenation delimiiter from the path expression.
 * @param {string} path concatenated JSONPath expressions to the requested nodes
 * @returns string value for the delimiter (first argument to the concat() function)
 */
function getConcatenatedDelimiter(path: string): string {
  return getConcatenatedPathArgs(path)!.shift()!.trim()
}

/**
 * Helper function to check if the JSONPath expression starts with the root symbol ('$').
 * @param {string} path JSONPath expression to evaluate
 * @returns boolean value indicating whether the JSONPath expression is rooted
 */
function hasRoot(path: string): boolean {
  return path.startsWith('$')
}
