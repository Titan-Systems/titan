-- CREATE SERVICE echo_service
--   IN COMPUTE POOL tutorial_compute_pool
--   FROM @tutorial_stage
--   SPECIFICATION_FILE='echo_spec.yaml'
--   MIN_INSTANCES=2
--   MAX_INSTANCES=2
--  ;

CREATE SERVICE titan_service_test
  IN COMPUTE POOL some_compute_pool
  FROM SPECIFICATION $$
spec:
  container:
  - name: container_name
    image: /some/image/path:latest
    env:
      PORT: 8000
      EXAMPLE_ENV_VARIABLE: my_value
  endpoint:
  - name: apiendpoint
    port: 8000
    public: true
$$
  MIN_INSTANCES=1
  MAX_INSTANCES=1
;