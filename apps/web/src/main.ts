import { createApp } from "vue";
import { createPinia } from "pinia";
import { PiniaColada } from "@pinia/colada";

import App from "./App.vue";
import { createAppRouter } from "./app/router";
import { initializeApiClient } from "./api/client";
import { useAuthStore } from "./auth";
import "./app/styles.css";

const app = createApp(App);
const pinia = createPinia();
const router = createAppRouter(pinia);
const auth = useAuthStore(pinia);

initializeApiClient({ getAccessToken: () => auth.accessToken });

app.use(pinia);
app.use(PiniaColada);
app.use(router);

app.mount("#app");
