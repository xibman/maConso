import {linky as linkySecrets, gazpar as grdfSecrets} from './secrets/secrets.json'
import {InfluxDB, Point} from '@influxdata/influxdb-client'
import {Session as LinkySession} from 'linky'
import {ConsommationType, GRDF} from 'grdf-api'
import {CronJob} from 'cron'
import {writeFileSync} from 'fs'

const {
    INFLUXDB_TOKEN, 
    INFLUXDB_URL,
    INFLUXDB_ORG,
    INFLUXDB_BUCKET,
    FIRST_RUN_START_DATE,
    LINKY_WITH_LOADCURVE
} = process.env

type LinkySecret = {accessToken: string, refreshToken: string, PRM: string}
type GRDFSecret = {username: string, password: string, PCE: string}

const dateToString = (date: Date = new Date()) => date.toISOString().split('T')[0]

async function getLinkyPoints({accessToken, refreshToken, PRM}: LinkySecret, start: string, withLoadCurve: boolean) {
    const session = new LinkySession({
        accessToken,
        refreshToken,
        usagePointId: PRM,
        onTokenRefresh: (newAT, newRT) => {
            writeFileSync('./secrets/secrets.json', JSON.stringify({gazpar: grdfSecrets, linky: [...linkySecrets.filter(s => s.PRM !== PRM), {accessToken: newAT, refreshToken: newRT, PRM}]}))
        }
    })

    const end = dateToString()
    const points = [
        ...(await session.getDailyConsumption(start, end)).data.map(day => 
            new Point('ENEDIS__ENERGIE_SOUTIRAGE')
                .floatField('kWh', day.value/1e3)
                .timestamp(new Date(day.date))
                .tag('PRM', PRM)
            ),
            ...(await session.getMaxPower(start, end)).data.map(day => 
                new Point('ENEDIS__PMAX_SOUTIRAGE')
                    .floatField('kVA', day.value/1e3)
                    .timestamp(new Date(day.date))
                    .tag('PRM', PRM)
                )
    ]
    if(withLoadCurve) points.push(...(await session.getLoadCurve(dateToString(new Date(Date.now() - 7*24*60*60e3)), end)).data.map(step =>
        new Point('ENEDIS__CDC_SOUTIRAGE')
            .floatField('kWh', step.value/1e3)
            .timestamp(new Date(step.date))
            .tag('PRM', PRM)
        ))
        return points
}

async function getGRDFPoints({username, password, PCE}: GRDFSecret, start: string) {
    const user = new GRDF(await GRDF.login(username, password))
    return (await user.getPCEConsumption(ConsommationType.informatives, [PCE], start, dateToString()))[PCE].releves.filter(r => r.energieConsomme !== null).map(r =>
                new Point('GRDF__CONSOMMATION')
                .floatField('kWh', r.energieConsomme)
                .floatField('m3', Math.round(r.energieConsomme/r.coeffConversion * 100) / 100)
                .timestamp(new Date(r.journeeGaziere))
                .tag('PCE', PCE))
}

let firstRun = true
const start = firstRun ? FIRST_RUN_START_DATE : dateToString(new Date(Date.now() - 7*24*60*60e3))

async function fetchData() {
    const writeApi = new InfluxDB({
        url: INFLUXDB_URL,
        token: INFLUXDB_TOKEN
    }).getWriteApi(INFLUXDB_ORG, INFLUXDB_BUCKET)

    for(const linkySecret of linkySecrets) {
        try {
            writeApi.writePoints(await getLinkyPoints(linkySecret, start, LINKY_WITH_LOADCURVE === 'true'))
        } catch(e) {
            console.error("Impossible d'obtenir les données d'électricité.")
        }
    }
    for(const grdfSecret of grdfSecrets) {
        try {
            writeApi.writePoints(await getGRDFPoints(grdfSecret, start))
        } catch(e) {
            console.error("Impossible d'obtenir les données de gaz.")
        }
    }
    await writeApi.close()
    
    
    if(firstRun) firstRun = false
}

fetchData()

new CronJob('0 1 * * *', fetchData).start()