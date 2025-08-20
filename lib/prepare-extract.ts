import { AthenaClient } from "@aws-sdk/client-athena";
import { Parser } from "@json2csv/plainjs";
import { configDotenv } from "dotenv";
import * as fs from "fs";

import { QueryInterface } from "./query-interface";
import { instance } from "./winston.logger";

export class ExtractGenerator {
  private athenaClient: AthenaClient;
  logger: typeof instance;
  constructor() {
    configDotenv();
    this.logger = instance;

    this.athenaClient = new AthenaClient({
      region: process.env.AWS_REGION || "us-west-1",
    });
  }
  async getApplicationData(): Promise<any[]> {
    const queryInterface: QueryInterface = new QueryInterface(
      this.athenaClient,
    );
    const applicationData = await queryInterface.executeQuery(
      `SELECT * FROM combined_applications_data`,
      "doorway-datalake",
    );
    this.logger.info(`Retrieved ${applicationData.length} applications`);
    return applicationData;
  }

  async getHouseholdMembers(): Promise<any[]> {
    const queryInterface: QueryInterface = new QueryInterface(
      this.athenaClient,
    );
    const householdMembers = await queryInterface.executeQuery(
      `SELECT * FROM household_members_no_pii`,
      "doorway-datalake",
    );
    this.logger.info(`Retrieved ${householdMembers.length} household members`);
    return householdMembers;
  }
  getLargestHousehold(members: any[]): number {
    //console.log(members)
    const householdCounts = new Map<string, number>();
    for (const member of members) {
      const currentCount = householdCounts.get(member.application_id) || 0;
      householdCounts.set(member.application_id, currentCount + 1);
    }
    let maxHouseholds: number = 0;
    householdCounts.forEach((count: number) => {
      if (count > maxHouseholds) {
        maxHouseholds = count;
      }
    });
    return maxHouseholds;
  }
  async getFullExtract() {
    const applications = await this.getApplicationData();
    const allHouseholdMembers = await this.getHouseholdMembers();

    // Calculate maxMembers once outside the loop
    const maxMembers: number = this.getLargestHousehold(allHouseholdMembers);

    // Pre-process household members by application ID for faster lookup
    const householdMembersByAppId = new Map<string, any[]>();
    allHouseholdMembers.forEach((member: any) => {
      if (!householdMembersByAppId.has(member.application_id)) {
        householdMembersByAppId.set(member.application_id, []);
      }
      householdMembersByAppId.get(member.application_id)!.push(member);
    });

    // Process applications in batches to reduce logging overhead
    const batchSize = 100;
    for (let i = 0; i < applications.length; i += batchSize) {
      const batch = applications.slice(i, i + batchSize);
      this.logger.info(
        `Processing applications ${i + 1} to ${Math.min(i + batchSize, applications.length)} of ${applications.length}`,
      );
      // Initialize counters

      batch.forEach((application) => {
        let numChildren = 0;
        let numSeniors = 0;
        const applicationId = application.application_id;
        const householdMembers =
          householdMembersByAppId.get(applicationId) || [];

        // Process all household members in one pass
        householdMembers.forEach((member, index) => {
          if (member.age <= 18) numChildren++;
          if (member.age >= 60) numSeniors++;

          if (index < maxMembers) {
            application[`household_age_${index}`] = member.age;
            application[`household_relationship_${index}`] =
              member.relationship;
          }
        });

        // Set counters
        application["number_children"] = numChildren;
        application["number_seniors"] = numSeniors;

        // Fill in NA values for missing household members
        for (let j = householdMembers.length; j < maxMembers; j++) {
          application[`household_age_${j}`] = "NA";
          application[`household_relationship_${j}`] = "NA";
        }
        const newPrefs: any[] = [];
        const preferences = JSON.parse(application["preferences"]);
        preferences.forEach((preference: any) => {
          const newOptions: any[] = [];
          preference["options"].forEach((option: any) => {
            delete option["extraData"];
            newOptions.push(option);
          });
          preference["options"] = newOptions;
          newPrefs.push(preference);
        });
        application["preferences"] = newPrefs;
      });
    }

    const parser = new Parser({
      fields: [
        "portal_url",
        "listing_id",
        "application_id",
        "submission_date",
        "submission_type",
        "application_language",
        "homeaddress_census_tract",
        "homeaddress_city",
        "homeaddress_state",
        "homeaddress_zip_code",
        "send_mail_to_mailing_address",
        "phone_number_type",
        "has_no_phone",
        "has_no_email",
        "alternatecontact_type",
        "alternatecontact_agency",
        "alternatecontact_other_type",
        "household_size",
        "income",
        "income_period",
        "income_vouchers",
        "accessibility_vision",
        "accessibility_mobility",
        "accessibility_hearing",
        "applicant_age",
        "student_in_household_almost_18",
        "household_expecting_changes",
        "race",
        "ethnicity",
        "gender",
        "sexual_orientation",
        "spoken_language",
        "how_did_you_hear",
        "programs",
        "preferences",
        "number_children",
        "number_seniors",
        "household_age_0",
        "household_relationship_0",
        "household_age_1",
        "household_relationship_1",
        "household_age_2",
        "household_relationship_2",
        "household_age_3",
        "household_relationship_3",
        "household_age_4",
        "household_relationship_4",
        "household_age_5",
        "household_relationship_5",
        "household_age_6",
        "household_relationship_6",
        "household_age_7",
        "household_relationship_7",
        "household_age_8",
        "household_relationship_8",
        "household_age_9",
        "household_relationship_9",
        "household_age_10",
        "household_relationship_10",
        "household_age_11",
        "household_relationship_11",
        "household_age_12",
        "household_relationship_12",
        "household_age_13",
        "household_relationship_13",
        "household_age_14",
        "household_relationship_14",
        "household_age_15",
        "household_relationship_15",
      ],
    });
    const csv = parser.parse(applications);
    fs.writeFileSync("extract.csv", csv);
    this.logger.info(
      `Extract generated with ${applications.length} applications to extract.csv`,
    );
  }
}
const generator = new ExtractGenerator();
generator.getFullExtract().then((extract) => generator.logger.info("Done!"));
