import Image from 'next/image';
import Link from 'next/link';

interface NavLink {
  label: string;
  href: string;
}

interface PageLayoutProps {
  title: string;
  heroImage: string;
  children: React.ReactNode;
  navLinks?: NavLink[];
}

export default function PageLayout({ title, heroImage, children, navLinks }: PageLayoutProps) {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="flex flex-col lg:flex-row gap-8">
        {/* Left: functional content */}
        <div className="flex-1 min-w-0 space-y-8">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">{title}</h1>
          {children}

          {/* Navigation buttons */}
          {navLinks && navLinks.length > 0 && (
            <div className="flex gap-4 pt-4 border-t border-gray-200 dark:border-gray-800">
              {navLinks.map(link => (
                <Link
                  key={link.href}
                  href={link.href}
                  className="px-5 py-2.5 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 font-medium rounded-lg transition-colors"
                >
                  {link.label} &rarr;
                </Link>
              ))}
            </div>
          )}
        </div>

        {/* Right: image */}
        <div className="hidden lg:block lg:w-80 xl:w-96 flex-shrink-0">
          <div className="sticky top-8">
            <Image
              src={heroImage}
              alt={title}
              width={400}
              height={600}
              className="rounded-xl object-contain w-full"
              priority
            />
          </div>
        </div>
      </div>
    </div>
  );
}
