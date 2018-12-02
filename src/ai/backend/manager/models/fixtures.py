from datetime import datetime


__all__ = ('example_keypair', 'example_scaling_group')


example_keypair = [
    (
        'keypairs',
        [
            {
                # Admin user example key
                'user_id': 'admin@lablup.com',
                'access_key': 'AKIAIOSFODNN7EXAMPLE',
                'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                'is_active': True,
                'billing_plan': 'free',
                'concurrency_limit': 30,
                'concurrency_used': 0,
                'rate_limit': 1000,
                'num_queries': 0,
                'is_admin': True,
            },
            {
                # Non-admin user example key
                'user_id': 'user@lablup.com',
                'access_key': 'AKIANABBDUSEREXAMPLE',
                'secret_key': 'C8qnIo29EZvXkPK_MXcuAakYTy4NYrxwmCEyNPlf',
                'is_active': True,
                'billing_plan': 'free',
                'concurrency_limit': 30,
                'concurrency_used': 0,
                'rate_limit': 1000,
                'num_queries': 0,
                'is_admin': False,
            },
        ]
    ),
]

example_scaling_group = [
    (
        'scaling_groups',
        [
            {
                'name': 'default',
                'is_active': True,
                'created_at': datetime.now(),
                'driver': '',
                'job_scheduler': '',
            },
            {
                'name': 'codeonweb',
                'is_active': True,
                'created_at': datetime.now(),
                'driver': '',
                'job_scheduler': '',
            }
        ]
    )
]
