import React from 'react';
import {
  getSpectrumComponent,
  SpectrumElementNode,
} from './SpectrumElementUtils';
import { ELEMENT_KEY } from './ElementUtils';

export type SpectrumElementViewProps = {
  element: SpectrumElementNode;
};

export function SpectrumElementView({
  element,
}: SpectrumElementViewProps): JSX.Element | null {
  const { [ELEMENT_KEY]: name, props = {} } = element;
  const Component = getSpectrumComponent(name);
  if (Component == null) {
    throw new Error(`Unknown Spectrum component ${name}`);
  }
  // eslint-disable-next-line react/prop-types
  const { children, ...otherProps } = props;
  // eslint-disable-next-line react/jsx-props-no-spreading, @typescript-eslint/no-explicit-any
  return <Component {...otherProps}>{children as any}</Component>;
}

export default SpectrumElementView;