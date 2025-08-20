import {
  AthenaClient,
  GetQueryExecutionCommand,
  GetQueryExecutionCommandOutput,
  GetQueryResultsCommand,
  GetQueryResultsCommandOutput,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";
import { mockClient } from "aws-sdk-client-mock";
import { QueryInterface } from "./query-interface";
describe("Run Athena query", () => {
  test("Submit Query", async () => {
    const athenaClient = new AthenaClient({ region: "us-west-1" });
    const athenaMock = mockClient(athenaClient);
    athenaMock.on(StartQueryExecutionCommand).resolves({
      QueryExecutionId: "foo",
    });
    const queryInterface = new QueryInterface(athenaClient);
    const queryId = await queryInterface.startQuery(
      "select * from foo",
      "doorway-datalake",
    );
    expect(queryId).toBe("foo");
  });
  test("Get Query Status", async () => {
    const athenaClient = new AthenaClient({ region: "us-west-1" });
    const athenaMock = mockClient(athenaClient);
    athenaMock.on(StartQueryExecutionCommand).resolves({
      QueryExecutionId: "foo",
    });
    const commandOutput: GetQueryExecutionCommandOutput = {
      $metadata: {},
      QueryExecution: {
        Status: { State: "SUCCEEDED" },
      },
    };
    athenaMock.on(GetQueryExecutionCommand).resolves(commandOutput);
    const queryInterface = new QueryInterface(athenaClient);
    const queryId = await queryInterface.startQuery(
      "select * from foo",
      "doorway-datalake",
    );
    const queryStatus = await queryInterface.getQueryStatus(queryId);
    expect(queryStatus).toBe("SUCCEEDED");
  });
  test("Get Output", async () => {
    const athenaClient = new AthenaClient({ region: "us-west-1" });
    const athenaMock = mockClient(athenaClient);
    athenaMock.on(StartQueryExecutionCommand).resolves({
      QueryExecutionId: "foo",
    });
    const commandOutput: GetQueryExecutionCommandOutput = {
      $metadata: {},
      QueryExecution: {
        Status: { State: "SUCCEEDED" },
      },
    };
    athenaMock.on(GetQueryExecutionCommand).resolves(commandOutput);
    const mockData: GetQueryResultsCommandOutput = {
      $metadata: {},
      ResultSet: {
        Rows: [
          {
            Data: [
              {
                VarCharValue: "foobar_column",
              },
              {
                VarCharValue: "barbash_column",
              },
            ],
          },
          {
            Data: [
              {
                VarCharValue: "foo",
              },
              {
                VarCharValue: "bash",
              },
            ],
          },
          {
            Data: [
              {
                VarCharValue: "bar",
              },
              {
                VarCharValue: "bar",
              },
            ],
          },
        ],
      },
    };
    athenaMock.on(GetQueryResultsCommand).resolves(mockData);
    const queryInterface = new QueryInterface(athenaClient);
    const queryId = await queryInterface.startQuery(
      "select * from foo",
      "doorway-datalake",
    );
    const mockResults: any[] = [
      {
        foobar_column: "foo",
        barbash_column: "bash",
      },
      {
        foobar_column: "bar",
        barbash_column: "bar",
      },
    ];
    const queryOutput = await queryInterface.getQueryOutput(queryId);
    expect(queryOutput).toStrictEqual(mockResults);
  });
  test("Full Query Test", async () => {
    const subQuerySpy = jest.spyOn(QueryInterface.prototype, "startQuery");
    subQuerySpy.mockReturnValue(Promise.resolve("foo"));
    const statusQuerySpy = jest.spyOn(
      QueryInterface.prototype,
      "getQueryStatus",
    );
    statusQuerySpy.mockReturnValue(Promise.resolve("SUCCEEDED"));
    const mockResults: any[] = [
      {
        foobar_column: "foo",
        barbash_column: "bash",
      },
      {
        foobar_column: "bar",
        barbash_column: "bar",
      },
    ];
    const outputQuerySpy = jest.spyOn(
      QueryInterface.prototype,
      "getQueryOutput",
    );
    outputQuerySpy.mockReturnValue(Promise.resolve(mockResults));
    const queryInterface: QueryInterface = new QueryInterface(
      new AthenaClient({ region: "us-west-1" }),
    );
    queryInterface.executeQuery("select * from foo", "doorway-datalake");
  });
});
