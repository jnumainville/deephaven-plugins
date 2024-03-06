import { WidgetDescriptor } from '@deephaven/dashboard';
import { TestUtils } from '@deephaven/utils';
import type { Widget } from '@deephaven/jsapi-types';

export function makeDocumentUpdatedJsonRpc(
  document: Record<string, unknown> = {}
) {
  return {
    jsonrpc: '2.0',
    method: 'documentUpdated',
    params: [JSON.stringify(document)],
  };
}

export function makeDocumentUpdatedJsonRpcString(
  document: Record<string, unknown> = {}
) {
  return JSON.stringify(makeDocumentUpdatedJsonRpc(document));
}

export function makeWidgetDescriptor({
  id = 'widget-id',
  type = 'widget-type',
  name = 'Widget Name',
} = {}): WidgetDescriptor {
  return {
    id,
    type,
    name,
  };
}

export function makeWidget({
  addEventListener = jest.fn(() => jest.fn()),
  getDataAsString = () => makeDocumentUpdatedJsonRpcString(),
  exportedObjects = [],
}: Partial<Widget> = {}): Widget {
  return TestUtils.createMockProxy<Widget>({
    addEventListener,
    getDataAsString,
    exportedObjects,
  });
}
