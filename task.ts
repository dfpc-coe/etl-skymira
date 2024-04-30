import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { FeatureCollection } from 'geojson';
import { Type, TSchema } from '@sinclair/typebox';

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'SKYMIRA_TOKEN': Type.String({ description: 'API Token for SkyMira API' }),
                'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false })
            });
        } else {
            return Type.Object({
            });
        }
    }

    async control() {
        const layer = await this.fetchLayer();

        if (!layer.environment.SKYMIRA_TOKEN) throw new Error('No SkyMira API Token Provided');

        const features: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        };

        await this.submit(features);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}
