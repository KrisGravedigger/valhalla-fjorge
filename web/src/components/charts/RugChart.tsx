'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts';
import type { MatchedPosition } from '@/lib/types';
import { aggregateDailyData, getWalletColors, shortWallet } from './chartUtils';

interface Props {
  positions: MatchedPosition[];
  hideLegend?: boolean;
}

export default function RugChart({ positions, hideLegend }: Props) {
  const { data, wallets } = useMemo(() => aggregateDailyData(positions, 'rugs'), [positions]);
  const colors = useMemo(() => getWalletColors(wallets), [wallets]);

  // Check if any rugs exist
  const hasRugs = data.some(d =>
    wallets.some(w => typeof d[w] === 'number' && (d[w] as number) > 0)
  );

  if (!hasRugs) return <p className="text-gray-500 text-center py-4">No rug events found</p>;

  return (
    <div className="bg-white dark:bg-gray-900 rounded-xl p-4 border border-gray-200 dark:border-gray-800">
      <h3 className="text-lg font-semibold mb-4">Daily Rug Count per Wallet</h3>
      <ResponsiveContainer width="100%" height={350}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
          <XAxis dataKey="date" tick={{ fontSize: 11 }} angle={-45} textAnchor="end" height={60} />
          <YAxis allowDecimals={false} tick={{ fontSize: 11 }} />
          <Tooltip />
          {!hideLegend && <Legend formatter={(value) => shortWallet(value)} />}
          {wallets.map(wallet => (
            <Line
              key={wallet}
              type="monotone"
              dataKey={wallet}
              stroke={colors[wallet]}
              name={wallet}
              dot={{ r: 3 }}
              strokeWidth={2}
              connectNulls={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
