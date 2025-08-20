import {
  AthenaClient,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  QueryExecutionState,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";
import { instance } from "./winston.logger";
export class QueryInterface {
  private athenaClient: AthenaClient;
  logger: typeof instance;
  constructor(client: AthenaClient) {
    this.athenaClient = client;
    this.logger = instance;
  }
  async startQuery(query: string, database: string): Promise<string> {
    const queryExecutionInput = {
      QueryString: query,
      QueryExecutionContext: {
        Database: database,
        Workgroup: "primary",
      },
    };
    const { QueryExecutionId } = await this.athenaClient.send(
      new StartQueryExecutionCommand(queryExecutionInput),
    );
    return QueryExecutionId || "ERROR";
  }
  async getQueryStatus(
    QueryExecutionId: string,
  ): Promise<QueryExecutionState | undefined> {
    const command = new GetQueryExecutionCommand({ QueryExecutionId });
    const execution = await this.athenaClient.send(command);
    const status = execution.QueryExecution!.Status!.State;
    return status;
  }
  async getQueryOutput(queryId: string): Promise<any[]> {
    const command = new GetQueryResultsCommand({ QueryExecutionId: queryId });
    const results = await this.athenaClient.send(command);
    const columns = results.ResultSet!.Rows![0];
    const mappedData: any[] = [];
    results.ResultSet!.Rows!.forEach((row, rowIndex) => {
      if (rowIndex > 0) {
        const mappedObject: any = {};
        row.Data!.forEach((cell, index) => {
          const key = columns.Data![index].VarCharValue!;
          mappedObject[key] = cell.VarCharValue!;
        });
        mappedData.push(mappedObject);
      }
    });
    let nextToken: string | undefined = results.NextToken;
    while (nextToken != undefined) {
      this.logger.debug(nextToken);
      const command = new GetQueryResultsCommand({
        QueryExecutionId: queryId,
        NextToken: nextToken,
      });
      const results = await this.athenaClient.send(command);
      results.ResultSet!.Rows!.forEach((row, rowIndex) => {
        if (rowIndex > 0) {
          const mappedObject: any = {};
          row.Data!.forEach((cell, index) => {
            const key = columns.Data![index].VarCharValue!;
            mappedObject[key] = cell.VarCharValue!;
          });
          mappedData.push(mappedObject);
        }
        nextToken = results.NextToken;
      });
    }
    return mappedData;
  }
  async executeQuery(query: string, database: string) {
    this.logger.info(`Executing query: ${query}`);
    const queryId = await this.startQuery(query, database);
    this.logger.debug(`Query ID: ${queryId}`);

    let running: boolean = true;
    while (running) {
      const status = await this.getQueryStatus(queryId);
      if (status == "SUCCEEDED") {
        running = false;
      } else if (status == "QUEUED" || status == "RUNNING") {
        this.logger.info(`Query ${queryId} is still running`);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } else {
        running = false;
        this.logger.error(`Query ${queryId} failed with status ${status}.`);
        throw new Error(`Query ${queryId} failed with status ${status}`);
      }
    }
    return await this.getQueryOutput(queryId);
  }
}
