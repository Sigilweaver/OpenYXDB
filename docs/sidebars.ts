import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
    docsSidebar: [
        'intro',
        'install',
        'quickstart',
        {
            type: 'category',
            label: 'Guide',
            collapsed: false,
            items: [
                'guide/reading',
                'guide/writing',
                'guide/polars',
                'guide/duckdb',
                'guide/field-types',
                'guide/low-level-api',
            ],
        },
        {
            type: 'category',
            label: 'Format',
            link: { type: 'doc', id: 'format/overview' },
            items: [
                'format/overview',
            ],
        },
        'changelog',
        'license',
    ],
};

export default sidebars;
