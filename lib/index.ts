import { parse } from "csv-parse/sync";
import { readFileSync } from "node:fs";
const input = readFileSync("./appextract.csv", "utf8");

const records = parse(input, {
  columns: true,
  skip_empty_lines: true,
});
const jsonStuff = JSON.parse(JSON.stringify(records))
let headers = Object.keys(jsonStuff[0])
headers = headers.filter((header: string) => header !== "preferences")
headers.push("preferences")
console.log(headers.join("~"))
jsonStuff.forEach((element: any) => {
  const preferences = JSON.parse(element["preferences"])
  const newPrefs: any[] = []
  preferences.forEach((preference: any) => {
    if (preference["claimed"] === true) {
      const options:[] = preference["options"]
      const newOptions:any[] = []
      options.forEach((option: any) => {
        delete option.extraData
        newOptions.push(option)
      })
      delete preference["options"]
      preference["options"] = newOptions
      newPrefs.push(JSON.stringify(preference))
    }
    delete element.preferences
    element["preferences"] = newPrefs
    console.log(Object.values(element).join("~"))
  })
})


