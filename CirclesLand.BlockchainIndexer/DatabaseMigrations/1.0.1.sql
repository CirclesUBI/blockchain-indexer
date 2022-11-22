create or replace view crc_capacity_graph_2("from", "to", token_owner, capacity, "limit") as
    WITH a AS (
             SELECT crc_current_trust_2.user_token,
                crc_current_trust_2.can_send_to,
                crc_current_trust_2.can_send_to_token,
                crc_current_trust_2."limit"
               FROM crc_current_trust_2
            ), b AS (
             SELECT bal.safe_address AS "from",
                a.can_send_to AS "to",
                a."limit",
                bal.token_owner,
                bal.balance AS from_balance
               FROM a
                 JOIN cache_crc_balances_by_safe_and_token bal ON bal.token = a.user_token
              WHERE bal.balance >= 0::numeric
            ), c AS (
             SELECT b."from",
                b."to",
                b."limit",
                b.token_owner,
                b.from_balance,
                bal.balance AS to_own_token_holdings
               FROM b
                 LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = b."to" AND bal.token_owner = b."to"
            ), d AS (
             SELECT c."from",
                c."to",
                c."limit",
                c.token_owner,
                c.from_balance,
                c.to_own_token_holdings,
                    CASE
                        WHEN bal.balance IS NOT NULL THEN bal.balance
                        ELSE 0::numeric
                    END AS to_already_holds_balance,
                os.organisation IS NOT NULL AS to_is_orga,
                c."to" = c.token_owner AS is_to_own_token
               FROM c
                 LEFT JOIN cache_crc_balances_by_safe_and_token bal ON bal.safe_address = c."to" AND bal.token_owner = c.token_owner
                 LEFT JOIN crc_organisation_signup_2 os ON os.organisation = c."to"
            ), e AS (
             SELECT d."from",
                d."to",
                d."limit",
                d.token_owner,
                d.from_balance,
                d.to_own_token_holdings,
                d.to_already_holds_balance,
                d.to_is_orga,
                d.is_to_own_token,
                    CASE
                        WHEN d.is_to_own_token OR d.to_is_orga THEN d.from_balance
                        ELSE d.to_own_token_holdings * d."limit" / 100::numeric
                    END AS max_transfer_amount
               FROM d
            ), f AS (
             SELECT e."from",
                e."to",
                e."limit",
                e.token_owner,
                e.from_balance,
                e.to_own_token_holdings,
                e.to_already_holds_balance,
                e.to_is_orga,
                e.is_to_own_token,
                    CASE
                        WHEN e."limit" > 0::numeric THEN e.max_transfer_amount
                        ELSE 0::numeric
                    END AS max_transfer_amount,
                e.to_already_holds_balance * (100::numeric - e."limit") / 100::numeric AS dest_balance_scaled
               FROM e
            ), g AS (
             SELECT f."from",
                f."to",
                f."limit",
                f.token_owner,
                f.from_balance,
                f.to_own_token_holdings,
                f.to_already_holds_balance,
                f.to_is_orga,
                f.is_to_own_token,
                f.max_transfer_amount,
                f.dest_balance_scaled,
                    CASE
                        WHEN f."limit" > 0::numeric THEN
                        CASE
                            WHEN (f.max_transfer_amount - f.dest_balance_scaled) > f.from_balance THEN f.from_balance
                            ELSE f.max_transfer_amount - f.dest_balance_scaled
                        END
                        ELSE 0::numeric
                    END AS capacity
               FROM f
            )
    SELECT g."from",
           g."to",
           g.token_owner,
           CASE
               WHEN g.to_is_orga OR g.is_to_own_token THEN g.max_transfer_amount
               ELSE
                   CASE
                       WHEN g.capacity < 0::numeric THEN 0::numeric
                    ELSE g.capacity
    END
    END AS capacity,
        g."limit"
       FROM g
      WHERE g."from" <> g."to";

insert into db_version (version, description) values ('1.0.0', 'The initial release-db');