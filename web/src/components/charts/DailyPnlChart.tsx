'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine,
} from 'recharts';
import type { MatchedPosition } from '@/lib/types';
import { aggregateDailyData, getWalletColors, shortWallet } from './chartUtils';

interface Props {
  positions: MatchedPosition[];
}

export default function DailyPnlChart({ positions }: Props) {
  const { data, wallets } = useMemo(() => aggregateDailyData(positions, 'pnl'), [positions]);
  const colors = useMemo(() => getWalletColors(wallets), [wallets]);

  if (data.length === 0) return <p className="text-gray-500 text-center py-4">No data for PnL chart</p>;

  return (
    <div className="bg-white dark:bg-gray-900 rounded-xl p-4 border border-gray-200 dark:border-gray-800">
      <h3 className="text-lg font-semibold mb-4">Daily PnL per Wallet (SOL)</h3>
      <ResponsiveContainer width="100%" height={400}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
          <XAxis dataKey="date" tick={{ fontSize: 11 }} angle={-45} textAnchor="end" height={60} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Legend formatter={(value) => shortWallet(value)} />
          <ReferenceLine y={0} stroke="#000" strokeOpacity={0.3} />
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
