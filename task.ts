import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';
import moment from 'moment';
import { FeatureCollection, Feature } from 'geojson';
import { Type, Static, TSchema } from '@sinclair/typebox';

const SkyMiraMessage = Type.Object({
    MobileID: Type.String(),
    vehicle_id: Type.String(),
    vehicle_type: Type.String(),
    agency: Type.String(),
    device_type: Type.String(),
    report_utc: Type.String(),
    received_utc: Type.String(),
    latitude: Type.Integer(),
    longitude: Type.Integer()
})

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'SKYMIRA_TOKEN': Type.String({ description: 'API Token for SkyMira API' }),
                'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false })
            });
        } else {
            return SkyMiraMessage
        }
    }

    async control() {
        const layer = await this.fetchLayer();

        if (!layer.environment.SKYMIRA_TOKEN) throw new Error('No SkyMira API Token Provided');

        const url = new URL('https://gpsgate.skymira.com/GPSClient/getforest_auth.php');
        url.searchParams.append('start_utc', moment().subtract(1, 'minute').toISOString())

        const res = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${layer.environment.SKYMIRA_TOKEN}`
            }
        });

        const body = await res.typed(Type.Object({
            ErrorId: Type.String(),
            NextStartUTC: Type.Union([Type.String(), Type.Integer()]),
            devices: Type.String(),
            Messages: Type.Union([Type.Array(SkyMiraMessage), Type.Null()])
        }));

        const features: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        };

        if (body.Messages === null) {
            console.error(JSON.stringify(body));
            throw new Error('SkyMira did return successful response')
        }

        // The list is a raw list of udpates so group into most current
        const agg: Map<string, Static<typeof SkyMiraMessage>> = new Map();
        for (const msg of body.Messages) {
            const current = agg.get(msg.MobileID);

            if ((current && moment(current.report_utc).isAfter(msg.report_utc)) || !current)  {
                agg.set(msg.MobileID, msg);
            }
        }

        for (const msg of agg.values()) {
            msg.longitude = msg.longitude / 60000;
            msg.latitude = msg.latitude / 60000;

            const feat: Feature = {
                id: `symira-${msg.MobileID}`,
                type: 'Feature',
                properties: {
                    callsign: msg.vehicle_id,
                    time: msg.received_utc,
                    start: msg.report_utc,
                    metadata: msg
                },
                geometry: {
                    type: 'Point',
                    coordinates: [ msg.longitude, msg.latitude ]
                }
            }

            features.features.push(feat);
        }

        await this.submit(features);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}
