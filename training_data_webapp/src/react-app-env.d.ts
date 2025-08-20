/// <reference types="react-scripts" />

// Type declarations for external modules
declare module 'react-plotly.js' {
  import * as React from 'react';
  import { PlotParams } from 'plotly.js';

  interface PlotProps extends Partial<PlotParams> {
    data: Partial<PlotParams['data']>;
    layout?: Partial<PlotParams['layout']>;
    frames?: Partial<PlotParams['frames']>;
    config?: Partial<PlotParams['config']>;
    style?: React.CSSProperties;
    className?: string;
    useResizeHandler?: boolean;
    onInitialized?: (figure: Readonly<PlotParams>, graphDiv: HTMLElement) => void;
    onUpdate?: (figure: Readonly<PlotParams>, graphDiv: HTMLElement) => void;
    onPurge?: (figure: Readonly<PlotParams>, graphDiv: HTMLElement) => void;
    onError?: (err: Error) => void;
  }

  const Plot: React.ComponentType<PlotProps>;
  export default Plot;
}