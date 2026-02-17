import type { Metadata } from 'next'
import Link from 'next/link'
import { Skranji } from 'next/font/google'
import './globals.css'

const skranji = Skranji({ weight: ['400', '700'], subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'Valhalla Fjorge',
  description: 'Analyze Your Valhalla Bot Performance',
}

function NavBar() {
  return (
    <nav className="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16 items-center">
          <Link href="/" className={`text-xl text-gray-900 dark:text-white ${skranji.className}`}>
            Valhalla Fjorge
          </Link>
          <div className="flex gap-6">
            <Link href="/parse" className="text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white transition-colors">
              Parse
            </Link>
            <Link href="/merge" className="text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white transition-colors">
              Merge
            </Link>
            <Link href="/charts" className="text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white transition-colors">
              Charts
            </Link>
          </div>
        </div>
      </div>
    </nav>
  )
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="min-h-screen flex flex-col">
        <NavBar />
        <main className="flex-1">
          {children}
        </main>
        <footer className="border-t border-gray-200 dark:border-gray-800 py-6 text-center text-sm text-gray-500 dark:text-gray-400 space-y-2">
          <p>All processing happens in your browser. Your data never leaves your device.</p>
          <p>
            For accurate PnL based on real Meteora API data, use the{' '}
            <a href="https://github.com/KrisGravedigger/valhalla-fjorge" className="underline font-medium hover:text-gray-700 dark:hover:text-gray-200">
              full CLI tool
            </a>.
          </p>
          <p>
            <a href="https://github.com/KrisGravedigger/valhalla-fjorge" className="underline hover:text-gray-700 dark:hover:text-gray-200">
              GitHub
            </a>
            {' '}&middot;{' '}
            Powered by Valhalla Bot
          </p>
        </footer>
      </body>
    </html>
  )
}
