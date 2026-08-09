import scrapy
import json


class LivresSpider(scrapy.Spider):
    name = "carrefour_livres"
    category_url = "https://www.carrefour.fr/r/livres"
    category_name = "Livres et Culture"
    max_pages = 50

    async def start(self):
        yield scrapy.Request(
            self.category_url,
            meta={"playwright": True, "page_num": 1},
            callback=self.parse,
        )

    def parse(self, response):
        page_num = response.meta["page_num"]
        self.logger.info(f"Page {page_num} reçue : {response.url}")

        products = self._extract_products(response.text)

        if products is None:
            self.logger.warning(
                f"Impossible d'extraire les produits sur {response.url} "
                f"(page {page_num}) — on continue quand même la pagination."
            )
        else:
            self.logger.info(f"{len(products)} produits trouvés sur la page {page_num}")

            n_no_ean = 0
            n_no_offer = 0
            n_yielded = 0

            for product_entry in products:
                attrs = product_entry.get("attributes", {})
                ean = attrs.get("ean")
                if not ean:
                    n_no_ean += 1
                    continue
                offer = self._get_carrefour_offer(attrs, ean)
                if offer:
                    n_yielded += 1
                    yield offer
                else:
                    n_no_offer += 1

            self.logger.info(
                f"Page {page_num} : {n_yielded} items yield, "
                f"{n_no_ean} sans EAN, {n_no_offer} sans aucune offre exploitable"
            )

        # On programme la page suivante même si l'extraction de la page
        # courante a échoué, pour ne pas casser toute la pagination sur
        # une seule page défaillante.
        if page_num < self.max_pages:
            next_url = f"{self.category_url}?page={page_num + 1}"
            yield scrapy.Request(
                next_url,
                meta={"playwright": True, "page_num": page_num + 1},
                callback=self.parse,
                dont_filter=True,
            )

    # ---------- Extraction de window.__INITIAL_STATE__ ----------

    def _extract_state(self, html):
        marker = "window.__INITIAL_STATE__"
        start_idx = html.find(marker)
        if start_idx == -1:
            return None

        eq_idx = html.find("=", start_idx)
        if eq_idx == -1:
            return None

        brace_start = html.find("{", eq_idx)
        if brace_start == -1:
            return None

        depth = 0
        in_string = False
        escape = False
        i = brace_start

        while i < len(html):
            char = html[i]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
            else:
                if char == '"':
                    in_string = True
                elif char == "{":
                    depth += 1
                elif char == "}":
                    depth -= 1
                    if depth == 0:
                        json_str = html[brace_start:i + 1]
                        try:
                            return json.loads(json_str)
                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON invalide (INITIAL_STATE) : {e}")
                            return None
            i += 1

        return None

    # ---------- Décodage du payload routeData (format 'devalue') ----------

    def _decode_route_data(self, route_data_str):
        """
        routeData est une chaîne JSON contenant un tableau au format 'devalue' :
        arr[0] est la racine, et chaque entier rencontré comme VALEUR dans un
        dict ou une liste est en réalité un INDEX pointant vers un autre
        élément du tableau (arr[index]) — jamais une valeur littérale directe.
        On reconstruit récursivement l'arbre réel à partir de ces références.
        """
        try:
            arr = json.loads(route_data_str)
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON invalide (routeData) : {e}")
            return None

        memo = {}

        def resolve(idx):
            if idx in memo:
                return memo[idx]
            if idx < 0 or idx >= len(arr):
                return None
            raw = arr[idx]
            if isinstance(raw, dict):
                result = {}
                memo[idx] = result
                for k, v in raw.items():
                    result[k] = resolve(v) if isinstance(v, int) else v
                return result
            elif isinstance(raw, list):
                result = []
                memo[idx] = result
                for v in raw:
                    result.append(resolve(v) if isinstance(v, int) else v)
                return result
            else:
                memo[idx] = raw
                return raw

        try:
            root = resolve(0)
        except RecursionError as e:
            self.logger.error(f"Erreur de décodage routeData (récursion) : {e}")
            return None

        return root

    def _extract_products(self, html):
        state = self._extract_state(html)
        if not state:
            self.logger.warning("Pas de __INITIAL_STATE__ exploitable")
            return None

        route_data_str = state.get("routeData")
        if not route_data_str:
            self.logger.warning("Pas de champ routeData dans __INITIAL_STATE__")
            return None

        root = self._decode_route_data(route_data_str)
        if not root:
            self.logger.warning("Échec du décodage de routeData")
            return None

        data = root.get("data")
        if not isinstance(data, list):
            self.logger.warning("root['data'] n'est pas une liste de produits")
            return None

        return data

    def _get_carrefour_offer(self, attrs, ean):
        offers = attrs.get("offers", {}).get(ean, {})
        if not offers:
            return None

        # 1) On cherche en priorité une offre vendue directement par Carrefour.
        chosen = None
        for offer in offers.values():
            if offer.get("subType") == "carrefour":
                chosen = offer
                break

        # 2) Sinon, on prend la première offre disponible (marketplace)
        #    plutôt que de perdre le produit.
        if chosen is None:
            chosen = next(iter(offers.values()), None)

        if chosen is None:
            return None

        try:
            price_info = chosen["attributes"]["price"]
            promo = chosen["attributes"].get("promotion")
            available = chosen["attributes"]["availability"]["purchasable"]
        except KeyError as e:
            self.logger.warning(f"EAN {ean} : structure d'offre inattendue, clé manquante {e}")
            return None

        return {
            "ean": ean,
            "title": attrs.get("title"),
            "brand": attrs.get("brand"),
            "category": attrs.get("topCategoryName"),
            "price": price_info.get("price"),
            "promo_price": (
                promo["messageArgs"]["discountedPrice"] if promo else None
            ),
            "available": available,
            "seller_type": chosen.get("subType"),  # "carrefour" ou autre (marketplace)
        }