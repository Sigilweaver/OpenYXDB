import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
    title: 'OpenYXDB',
    tagline: 'Python and C++ reader/writer for Alteryx YXDB files',
    favicon: 'img/favicon.ico',

    markdown: {
        mermaid: true,
        hooks: {
            onBrokenMarkdownLinks: 'warn',
        },
    },
    themes: ['@docusaurus/theme-mermaid'],

    url: 'https://sigilweaver.app',
    baseUrl: '/openyxdb/docs/',

    organizationName: 'Sigilweaver',
    projectName: 'OpenYXDB',

    onBrokenLinks: 'throw',

    i18n: {
        defaultLocale: 'en',
        locales: ['en'],
    },

    presets: [
        [
            'classic',
            {
                docs: {
                    routeBasePath: '/',
                    sidebarPath: './sidebars.ts',
                    editUrl: 'https://github.com/Sigilweaver/OpenYXDB/tree/main/docs/',
                },
                blog: false,
                sitemap: {
                    changefreq: 'weekly',
                    priority: 0.5,
                    filename: 'sitemap.xml',
                },
                theme: {
                    customCss: './src/css/custom.css',
                },
            } satisfies Preset.Options,
        ],
    ],

    themeConfig: {
        metadata: [
            { name: 'keywords', content: 'OpenYXDB, Alteryx, YXDB, Python, C++, PyArrow, Pandas, Polars, DuckDB, data engineering' },
            { name: 'description', content: 'OpenYXDB is a Python and C++ reader/writer for Alteryx YXDB files with PyArrow, Pandas, Polars, and DuckDB integration.' },
        ],
        colorMode: {
            defaultMode: 'dark',
            disableSwitch: false,
            respectPrefersColorScheme: true,
        },
        navbar: {
            title: 'Sigilweaver',
            logo: {
                alt: 'Sigilweaver logo',
                src: 'img/logo.svg',
                href: 'https://sigilweaver.app',
                target: '_self',
            },
            items: [
                {
                    type: 'dropdown',
                    label: 'OpenYXDB',
                    position: 'left',
                    items: [
                        { label: 'OpenProteo', href: 'https://sigilweaver.app/openproteo/docs/' },
                        { label: 'OpenTFRaw (Thermo)', href: 'https://sigilweaver.app/opentfraw/docs/' },
                        { label: 'OpenTimsTDF (Bruker)', href: 'https://sigilweaver.app/opentimstdf/docs/' },
                        { label: 'OpenWRaw (Waters)', href: 'https://sigilweaver.app/openwraw/docs/' },
                    ],
                },
                {
                    href: 'https://pypi.org/project/openyxdb/',
                    label: 'PyPI',
                    position: 'right',
                },
                {
                    href: 'https://sigilweaver.app',
                    label: 'Website',
                    position: 'right',
                },
                {
                    href: 'https://github.com/Sigilweaver/OpenYXDB',
                    label: 'GitHub',
                    position: 'right',
                },
            ],
        },
        footer: {
            style: 'dark',
            links: [
                {
                    title: 'Project',
                    items: [
                        { label: 'GitHub', href: 'https://github.com/Sigilweaver/OpenYXDB' },
                        { label: 'Issues', href: 'https://github.com/Sigilweaver/OpenYXDB/issues' },
                        { label: 'PyPI', href: 'https://pypi.org/project/openyxdb/' },
                        { label: 'Changelog', href: 'https://github.com/Sigilweaver/OpenYXDB/blob/main/CHANGELOG.md' },
                    ],
                },
                {
                    title: 'Sigilweaver',
                    items: [
                        { label: 'Website', href: 'https://sigilweaver.app' },
                        { label: 'Other projects', href: 'https://sigilweaver.app#projects' },
                    ],
                },
                {
                    title: 'Legal',
                    items: [
                        { label: 'Terms of Use', href: 'https://sigilweaver.app/terms' },
                        { label: 'Privacy Policy', href: 'https://sigilweaver.app/privacy' },
                    ],
                },
            ],
            copyright: `Copyright ${new Date().getFullYear()} Sigilweaver Holdings LLC. OpenYXDB is GPL-3.0 licensed (derived from Alteryx open-source). Documentation licensed under <a href="https://creativecommons.org/licenses/by-sa/4.0/" target="_blank" rel="noopener noreferrer">CC-BY-SA 4.0</a>.`,
        },
        prism: {
            theme: prismThemes.github,
            darkTheme: prismThemes.dracula,
            additionalLanguages: ['python', 'bash', 'toml', 'cmake'],
        },
    } satisfies Preset.ThemeConfig,
};

export default config;
