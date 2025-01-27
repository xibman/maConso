import {
  linky as linkySecrets,
  gazpar as grdfSecrets,
} from "./secrets/secrets.json";
import { InfluxDB, Point } from "@influxdata/influxdb-client";
import { Session as LinkySession } from "linky";
import { ConsommationType, GRDF } from "grdf-api";
import { CronJob } from "cron";
import { writeFileSync } from "fs";
import dayjs, { Dayjs } from "dayjs";
import utc from "dayjs/plugin/utc";
import isBetween from "dayjs/plugin/isBetween";
import objectSupport from "dayjs/plugin/objectSupport";
dayjs.extend(utc);
dayjs.extend(isBetween);
dayjs.extend(objectSupport);

const { INFLUXDB_TOKEN, INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET } =
  process.env;

type Hc = [
  {
    start: {
      minutes: string;
      hours: string;
    };
    end: {
      minutes: string;
      hours: string;
    };
  }
];

type LinkySecret = {
  accessToken: string;
  refreshToken: string;
  PRM: string;
  isLoadCurve: boolean;
  hc: Hc;
};

type GRDFSecret = { username: string; password: string; PCE: string };

const dateToString = (date: Dayjs = dayjs()) => date.format("YYYY-MM-DD");

async function getLinkyPoints(
  { accessToken, refreshToken, PRM, isLoadCurve, hc }: LinkySecret,
  start: Dayjs
) {
  const formatedStart = dateToString(start);
  const session = new LinkySession({
    accessToken,
    refreshToken,
    usagePointId: PRM,
    onTokenRefresh: (newAT, newRT) => {
      // @ts-ignore
      const newLinkySecrets: any = linkySecrets.filter((s) => s.PRM !== PRM);
      if (newAT !== "" && newRT !== "")
        newLinkySecrets.push({
          accessToken: newAT,
          refreshToken: newRT,
          PRM,
          isLoadCurve,
          hc,
        });
      writeFileSync(
        "./secrets/secrets.json",
        JSON.stringify({ gazpar: grdfSecrets, linky: newLinkySecrets })
      );
    },
  });

  const end = dayjs();
  const formatedEnd = dateToString(end);
  const points = [
    ...(await session.getDailyConsumption(formatedStart, formatedEnd)).data.map(
      (day) =>
        new Point("ENEDIS__ENERGIE_SOUTIRAGE")
          .floatField("kWh", day.value / 1e3)
          .timestamp(new Date(day.date))
          .tag("PRM", PRM)
    ),
    ...(await session.getMaxPower(formatedStart, formatedEnd)).data.map((day) =>
      new Point("ENEDIS__PMAX_SOUTIRAGE")
        .floatField("kVA", day.value / 1e3)
        .timestamp(new Date(day.date))
        .tag("PRM", PRM)
    ),
  ];
  if (isLoadCurve) {
    const weeksDifference = end.diff(start, "week");

    for (let week = 0; week < weeksDifference; week++) {
      const startCDC = dateToString(start.add(week, "week"));
      const startCDCTimePlusPeriod = start.add(week + 1, "week");
      const endCDC = dateToString(
        startCDCTimePlusPeriod.isAfter(end) ? end : startCDCTimePlusPeriod
      );

      console.log(`LOG(${PRM}): CDC PERIOD: ${startCDC} -> ${endCDC}`);

      try {
        points.push(
          ...(await session.getLoadCurve(startCDC, endCDC)).data.map((step) => {
            const pointTimestamp = dayjs(step.date);
            const point = new Point("ENEDIS__CDC_SOUTIRAGE")
              .floatField("kW", step.value / 1e3)
              .timestamp(pointTimestamp.utc().toDate())
              .tag("PRM", PRM);

            let heuresCreuses = 0;
            let heuresPleines = 0;
            let heuresNormales = 0;

            if (hc.length > 0) {
              const testResult: boolean[] = [];
              hc.forEach((hcSlice) => {
                testResult.push(
                  pointTimestamp.isBetween(
                    pointTimestamp.set(hcSlice.start),
                    pointTimestamp.set(hcSlice.end)
                  )
                );
              });
              if (testResult.includes(true)) {
                heuresCreuses = 1;
              } else {
                heuresPleines = 1;
              }
            } else {
              heuresNormales = 1;
            }
            point.tag("heures_pleines", heuresPleines.toString());
            point.tag("heures_creuses", heuresCreuses.toString());
            point.tag("heures_normales", heuresNormales.toString());
            return point;
          })
        );
      } catch (e) {
        console.log(
          `ERREUR(${PRM}): Impossible de récupérer la courbe de charge pour la période ${startCDC} -> ${endCDC}.`
        );
      }
    }
  }
  return points;
}

async function getGRDFPoints(
  { username, password, PCE }: GRDFSecret,
  start: Dayjs
) {
  const user = new GRDF(await GRDF.login(username, password));
  return (
    await user.getPCEConsumption(
      ConsommationType.informatives,
      [PCE],
      dateToString(start),
      dateToString()
    )
  )[PCE].releves
    .filter((r) => r.energieConsomme !== null)
    .map((r) =>
      new Point("GRDF__CONSOMMATION")
        .floatField("kWh", r.energieConsomme)
        .floatField(
          "m3",
          Math.round((r.energieConsomme / r.coeffConversion) * 100) / 100
        )
        .timestamp(dayjs(r.journeeGaziere).utc().toDate())
        .tag("PCE", PCE)
    );
}

async function fetchData(firstRun: boolean = false) {
  const writeApi = new InfluxDB({
    url: INFLUXDB_URL,
    token: INFLUXDB_TOKEN,
  }).getWriteApi(INFLUXDB_ORG, INFLUXDB_BUCKET);

  const start = firstRun
    ? dayjs().subtract(1, "year")
    : dayjs().subtract(7, "day");
  for (const linkySecret of linkySecrets) {
    try {
      writeApi.writePoints(await getLinkyPoints(linkySecret, start));
      console.log(`SUCCES(${linkySecret.PRM}): Relevés d'électricité obtenus.`);
    } catch (e) {
      console.log(
        `ERREUR(${linkySecret.PRM}): Impossible d'obtenir les données d'électricité.`
      );
    }
  }
  for (const grdfSecret of grdfSecrets) {
    try {
      writeApi.writePoints(await getGRDFPoints(grdfSecret, start));
      console.log(`SUCCES(${grdfSecret.PCE}): Relevés de gaz obtenus.`);
    } catch (e) {
      console.log(
        `ERREUR(${grdfSecret.PCE}): Impossible d'obtenir les données de gaz.`
      );
    }
  }
  await writeApi.close();
  console.log("SUCCES: Base de données mise à jour.");
}

fetchData(true);

new CronJob("0 8 * * *", fetchData).start();
