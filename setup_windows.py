"""Create Windows executables and installers for Kukur."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

from cx_Freeze import setup, Executable

executables = [
    Executable(
        'kukur/cli.py',
        target_name='kukur.exe',
    ),
    Executable(
        'windows/win_service_config.py',
        base='Win32Service',
        target_name='kukur-service.exe',
    ),
]

bdist_msi_options = {
    'upgrade_code': '{859ffb57-14ca-4771-8858-ca6ee86c6400}',
    'all_users': True,
    "summary_data": {
        "author": "Timeseer.AI",
        "comments": "Kukur",
    },
}

setup(
    name="Kukur",
    version="0.1",
    description="Kukur",
    executables=executables,
    options={
        "build_exe": {
            'include_msvcr': True,
            "includes": [
                'cx_Logging',
                'windows.win_service',
            ],
            "include_files": [
                ('windows/Kukur-windows.toml', 'Kukur-example.toml')
            ],
        },
        "bdist_msi": bdist_msi_options,
    },
)
