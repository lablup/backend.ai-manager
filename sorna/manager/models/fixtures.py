__all__ = ('example_keypair', )


example_keypair = [
    (
        'keypairs',
        [
            {
                'user_id': 2,  # 1: anonymouse user, 2: default super user
                'access_key': 'AKIAIOSFODNN7EXAMPLE',
                'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                'is_active': True,
                'billing_plan': 'free',
                'concurrency_limit': 30,
                'concurrency_used': 0,
                'rate_limit': 1000,
                'num_queries': 0,
                'is_admin': True,
                # Sample free tier: 500 launches per day x 30 days per month
                # 'remaining_cpu': 180000 * 500 * 30,   # msec (180 sec per launch)
                # 'remaining_mem': 1048576 * 500 * 30,  # KBytes (1GB per launch)
                # 'remaining_io': 102400 * 500 * 30,    # KBytes (100MB per launch)
                # 'remaining_net': 102400 * 500 * 30,   # KBytes (100MB per launch)
            },
        ]
    ),
]
