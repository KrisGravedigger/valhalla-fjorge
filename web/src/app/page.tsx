import Link from 'next/link'
import { Skranji } from 'next/font/google'

const skranji = Skranji({ weight: ['400', '700'], subsets: ['latin'] })

const features = [
  {
    title: 'Parse Logs',
    description: 'Paste Discord DMs or upload log files to extract position data into CSV.',
    href: '/parse',
    icon: '📋',
  },
  {
    title: 'Merge CSVs',
    description: 'Combine multiple positions.csv files with automatic deduplication.',
    href: '/merge',
    icon: '🔀',
  },
  {
    title: 'Charts',
    description: 'Visualize PnL, win rate, entries, rugs, and rolling averages.',
    href: '/charts',
    icon: '📊',
  },
]

export default function HomePage() {
  return (
    <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
      <div className="text-center mb-12">
        <h1 className={`text-6xl sm:text-7xl text-gray-900 dark:text-white mb-4 ${skranji.className}`}>
          Valhalla Fjorge
        </h1>
        <p className="text-xl text-gray-600 dark:text-gray-300 mb-4">
          Analyze Your Valhalla Bot Performance
        </p>
        <p className="text-sm text-gray-500 dark:text-gray-400 bg-green-50 dark:bg-green-950 inline-block px-4 py-2 rounded-full">
          All processing happens in your browser &mdash; your data never leaves your device
        </p>
      </div>

      {/* Concept demo notice */}
      <div className="max-w-2xl mx-auto mb-12 p-5 bg-gray-50 dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 text-sm text-gray-600 dark:text-gray-400 space-y-3">
        <p>
          <strong className="text-gray-900 dark:text-white">This is a lightweight preview tool.</strong>{' '}
          PnL values shown here are approximations based on Discord log data (wallet balance before/after each transaction).
          They can differ from actual on-chain results.
        </p>
        <p>
          For accurate PnL, the{' '}
          <a href="https://github.com/KrisGravedigger/valhalla-fjorge" className="underline font-medium hover:text-gray-900 dark:hover:text-gray-200">
            full CLI tool
          </a>{' '}
          fetches real deposit, withdrawal, and fee data directly from the Meteora DLMM API.
          Querying the Meteora API repeatedly from a browser would quickly trigger rate limits and IP blocks,
          so this web version relies on the approximate values from bot logs instead.
        </p>
      </div>

      <div className="grid md:grid-cols-3 gap-8 mb-12">
        {features.map((feature) => (
          <Link
            key={feature.href}
            href={feature.href}
            className="block p-8 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 hover:border-blue-400 dark:hover:border-blue-500 hover:shadow-lg transition-all group"
          >
            <div className="text-4xl mb-4">{feature.icon}</div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-2 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
              {feature.title}
            </h2>
            <p className="text-gray-600 dark:text-gray-400">
              {feature.description}
            </p>
          </Link>
        ))}
      </div>
    </div>
  )
}
